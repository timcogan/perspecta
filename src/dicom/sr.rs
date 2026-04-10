use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use dicom_object::{DefaultDicomObject, InMemDicomObject, Tag};

use super::{
    classify_dicom_object, collect_full_metadata, collect_metadata, open_dicom_object,
    read_item_multi_float, read_item_multi_int, read_item_string, read_string,
    sequence_items_from_item, sequence_items_from_object, DicomPathKind, DicomSource,
    FullMetadataField, GspsGraphic, GspsUnits, MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SrRenderingIntent {
    PresentationRequired,
    PresentationOptional,
    NotForPresentation,
}

impl SrRenderingIntent {
    pub fn is_visible_in_v1(self) -> bool {
        matches!(self, Self::PresentationRequired)
    }

    fn from_code(code: &SrCode) -> Option<Self> {
        if code_token_matches(code, "PRESENTATIONREQUIRED") {
            return Some(Self::PresentationRequired);
        }
        if code_token_matches(code, "PRESENTATIONOPTIONAL") {
            return Some(Self::PresentationOptional);
        }
        if code_token_matches(code, "NOTFORPRESENTATION") {
            return Some(Self::NotForPresentation);
        }
        None
    }
}

fn code_token_matches(code: &SrCode, expected: &str) -> bool {
    let expected = normalize_code_token(expected);
    code.meaning
        .as_deref()
        .is_some_and(|meaning| normalize_code_token(meaning) == expected)
        || code
            .value
            .as_deref()
            .is_some_and(|value| normalize_code_token(value) == expected)
}

#[derive(Debug, Clone)]
pub struct SrOverlayGraphic {
    pub graphic: GspsGraphic,
    pub referenced_frames: Option<Vec<usize>>,
    pub rendering_intent: SrRenderingIntent,
    #[allow(dead_code)]
    pub cad_operating_point: Option<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SrOverlayLabel {
    /// Anchor position in the coordinate space described by `units`.
    pub anchor: (f32, f32),
    pub units: GspsUnits,
    pub lines: Vec<String>,
    pub referenced_frames: Option<Vec<usize>>,
    pub rendering_intent: SrRenderingIntent,
}

#[derive(Debug, Clone, Default)]
pub struct SrOverlay {
    pub graphics: Vec<SrOverlayGraphic>,
    pub labels: Vec<SrOverlayLabel>,
}

impl SrOverlay {
    pub fn is_empty(&self) -> bool {
        self.graphics.is_empty() && self.labels.is_empty()
    }

    pub fn graphics_for_frame(
        &self,
        frame_index: usize,
    ) -> impl Iterator<Item = &SrOverlayGraphic> + '_ {
        self.graphics.iter().filter(move |graphic| {
            sr_overlay_applies_to_frame(graphic.referenced_frames.as_ref(), frame_index)
        })
    }

    pub fn visible_graphics_for_frame(
        &self,
        frame_index: usize,
    ) -> impl Iterator<Item = &GspsGraphic> + '_ {
        self.graphics_for_frame(frame_index).filter_map(|graphic| {
            graphic
                .rendering_intent
                .is_visible_in_v1()
                .then_some(&graphic.graphic)
        })
    }

    pub fn labels_for_frame(
        &self,
        frame_index: usize,
    ) -> impl Iterator<Item = &SrOverlayLabel> + '_ {
        self.labels.iter().filter(move |label| {
            sr_overlay_applies_to_frame(label.referenced_frames.as_ref(), frame_index)
        })
    }

    pub fn visible_labels_for_frame(
        &self,
        frame_index: usize,
    ) -> impl Iterator<Item = &SrOverlayLabel> + '_ {
        self.labels_for_frame(frame_index)
            .filter(|label| label.rendering_intent.is_visible_in_v1())
    }
}

fn sr_overlay_applies_to_frame(referenced_frames: Option<&Vec<usize>>, frame_index: usize) -> bool {
    let dicom_frame_number = frame_index.saturating_add(1);
    match referenced_frames {
        Some(frames) => frames.contains(&dicom_frame_number),
        None => true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredReportNode {
    pub relationship_type: Option<String>,
    pub label: String,
    pub value: Option<String>,
    pub children: Vec<StructuredReportNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredReportDocument {
    pub title: String,
    pub modality: Option<String>,
    pub completion_flag: Option<String>,
    pub verification_flag: Option<String>,
    pub content: Vec<StructuredReportNode>,
    pub metadata: Vec<(String, String)>,
    pub full_metadata: Arc<[FullMetadataField]>,
}

#[derive(Debug, Clone, Default)]
struct SrCode {
    value: Option<String>,
    meaning: Option<String>,
}

impl SrCode {
    fn matches_meaning(&self, expected: &str) -> bool {
        self.meaning
            .as_deref()
            .is_some_and(|meaning| normalize_code_token(meaning) == normalize_code_token(expected))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReferencedImageTarget {
    sop_instance_uid: String,
    referenced_frames: Option<Vec<usize>>,
    laterality: Option<String>,
    view: Option<String>,
}

#[derive(Debug, Clone)]
struct SrSpatialCoordinates {
    graphic_type: String,
    points: Vec<(f32, f32)>,
    units: GspsUnits,
}

#[derive(Debug, Clone)]
struct SrIndexedNode {
    path: Vec<usize>,
    relationship_type: Option<String>,
    referenced_content_item_identifier: Option<Vec<usize>>,
    concept_name: SrCode,
    coded_value: SrCode,
    numeric_value: Option<f32>,
    image_reference: Option<ReferencedImageTarget>,
    spatial_coordinates: Option<SrSpatialCoordinates>,
    children: Vec<SrIndexedNode>,
}

impl SrIndexedNode {
    fn is_feature_or_finding(&self) -> bool {
        self.concept_name.matches_meaning("Composite Feature")
            || self.concept_name.matches_meaning("Single Image Finding")
    }

    fn rendering_intent(&self) -> Option<SrRenderingIntent> {
        self.concept_modifiers()
            .filter(|child| child.concept_name.matches_meaning("Rendering Intent"))
            .find_map(|child| SrRenderingIntent::from_code(&child.coded_value))
    }

    fn cad_operating_point(&self) -> Option<f32> {
        self.concept_modifiers()
            .filter(|child| child.concept_name.matches_meaning("CAD Operating Point"))
            .find_map(|child| child.numeric_value)
    }

    fn property_geometry_nodes<'a>(&'a self, nodes: &mut Vec<&'a SrIndexedNode>) {
        for child in self.property_children() {
            if child.spatial_coordinates.is_some() {
                nodes.push(child);
            }
            child.property_geometry_nodes(nodes);
        }
    }

    fn selected_image_target<'a>(
        &'a self,
        image_index: &'a HashMap<Vec<usize>, ReferencedImageTarget>,
    ) -> Option<ReferencedImageTarget> {
        for child in &self.children {
            if let Some(mut reference) = child.image_reference.clone() {
                hydrate_image_context(&mut reference, child);
                return Some(reference);
            }
            if let Some(reference_path) = child.referenced_content_item_identifier.as_ref() {
                if let Some(reference) = image_index.get(reference_path) {
                    return Some(reference.clone());
                }
            }
        }
        None
    }

    fn concept_modifiers(&self) -> impl Iterator<Item = &SrIndexedNode> {
        self.children.iter().filter(|child| {
            relationship_type_matches(child.relationship_type.as_deref(), "HAS CONCEPT MOD")
        })
    }

    fn property_children(&self) -> impl Iterator<Item = &SrIndexedNode> {
        self.children.iter().filter(|child| {
            relationship_type_matches(child.relationship_type.as_deref(), "HAS PROPERTIES")
        })
    }

    fn acquisition_context_children(&self) -> impl Iterator<Item = &SrIndexedNode> {
        self.children.iter().filter(|child| {
            relationship_type_matches(child.relationship_type.as_deref(), "HAS ACQ CONTEXT")
        })
    }

    fn certainty_of_finding(&self) -> Option<f32> {
        self.property_children()
            .filter(|child| child.concept_name.matches_meaning("Certainty of Finding"))
            .find_map(|child| child.numeric_value)
    }

    fn acquisition_context_value(&self, concept_name: &str) -> Option<String> {
        self.acquisition_context_children()
            .find(|child| child.concept_name.matches_meaning(concept_name))
            .and_then(|child| {
                child
                    .coded_value
                    .meaning
                    .clone()
                    .or_else(|| child.coded_value.value.clone())
            })
    }
}

impl StructuredReportDocument {
    #[cfg(test)]
    pub(crate) fn test_stub() -> Self {
        Self {
            title: "Structured Report".to_string(),
            modality: Some("SR".to_string()),
            completion_flag: Some("COMPLETE".to_string()),
            verification_flag: Some("UNVERIFIED".to_string()),
            content: vec![StructuredReportNode {
                relationship_type: None,
                label: "Findings".to_string(),
                value: Some("No acute abnormality".to_string()),
                children: Vec::new(),
            }],
            metadata: vec![
                ("Modality".to_string(), "SR".to_string()),
                (
                    "SeriesDescription".to_string(),
                    "Structured Report".to_string(),
                ),
            ],
            full_metadata: Arc::default(),
        }
    }
}

pub fn load_structured_report(source: impl Into<DicomSource>) -> Result<StructuredReportDocument> {
    let source = source.into();
    let obj = open_dicom_object(&source)?;
    if classify_dicom_object(&obj) != DicomPathKind::StructuredReport {
        let sop_class = read_string(&obj, "SOPClassUID").unwrap_or_else(|| "unknown".to_string());
        bail!(
            "{} is not a Structured Report object (SOPClassUID={})",
            source,
            sop_class
        );
    }
    Ok(parse_structured_report_document(&obj))
}

pub fn load_mammography_cad_sr_overlays(
    source: impl Into<DicomSource>,
) -> Result<HashMap<String, SrOverlay>> {
    let source = source.into();
    let obj = open_dicom_object(&source)?;
    if classify_dicom_object(&obj) != DicomPathKind::StructuredReport {
        let sop_class = read_string(&obj, "SOPClassUID").unwrap_or_else(|| "unknown".to_string());
        bail!(
            "{} is not a Structured Report object (SOPClassUID={})",
            source,
            sop_class
        );
    }
    Ok(parse_mammography_cad_sr_overlays(&obj))
}

pub(crate) fn parse_mammography_cad_sr_overlays(
    obj: &DefaultDicomObject,
) -> HashMap<String, SrOverlay> {
    const CONTENT_SEQUENCE: Tag = Tag(0x0040, 0xA730);

    if read_string(obj, "SOPClassUID").as_deref() != Some(MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID) {
        return HashMap::new();
    }

    let Some(items) = sequence_items_from_object(obj, CONTENT_SEQUENCE) else {
        return HashMap::new();
    };

    // SR content item identifiers are rooted at the document container, which is
    // encoded in the dataset itself rather than inside ContentSequence.
    let nodes = build_indexed_nodes(items, &[1]);
    let mut image_index = HashMap::new();
    collect_image_references(&nodes, &mut image_index);

    let mut overlays = HashMap::<String, SrOverlay>::new();
    collect_mammography_cad_overlays(&nodes, &image_index, &mut overlays);
    overlays.retain(|_, overlay| !overlay.is_empty());
    overlays
}

pub(crate) fn parse_structured_report_document(
    obj: &DefaultDicomObject,
) -> StructuredReportDocument {
    const CONTENT_SEQUENCE: Tag = Tag(0x0040, 0xA730);

    let mut content = sequence_items_from_object(obj, CONTENT_SEQUENCE)
        .map(parse_structured_report_nodes)
        .unwrap_or_default();

    let mut title = content
        .first()
        .map(|node| node.label.clone())
        .filter(|label| !label.is_empty() && label != "Container")
        .or_else(|| read_string(obj, "SeriesDescription"))
        .or_else(|| read_string(obj, "StudyDescription"))
        .unwrap_or_else(|| "Structured Report".to_string());

    if content.len() == 1 {
        let root = content.remove(0);
        if root.relationship_type.is_none() && root.value.is_none() && !root.children.is_empty() {
            title = if root.label.is_empty() {
                title
            } else {
                root.label
            };
            content = root.children;
        } else {
            content = vec![root];
        }
    }

    StructuredReportDocument {
        title,
        modality: read_string(obj, "Modality"),
        completion_flag: read_string(obj, "CompletionFlag"),
        verification_flag: read_string(obj, "VerificationFlag"),
        content,
        metadata: collect_metadata(obj),
        full_metadata: collect_full_metadata(obj).into(),
    }
}

fn build_indexed_nodes(items: &[InMemDicomObject], parent_path: &[usize]) -> Vec<SrIndexedNode> {
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let mut path = parent_path.to_vec();
            path.push(index.saturating_add(1));
            parse_indexed_node(item, path)
        })
        .collect()
}

fn parse_indexed_node(item: &InMemDicomObject, path: Vec<usize>) -> SrIndexedNode {
    const CONTENT_SEQUENCE: Tag = Tag(0x0040, 0xA730);
    const RELATIONSHIP_TYPE: Tag = Tag(0x0040, 0xA010);
    const VALUE_TYPE: Tag = Tag(0x0040, 0xA040);
    const REFERENCED_CONTENT_ITEM_IDENTIFIER: Tag = Tag(0x0040, 0xDB73);
    const CONCEPT_NAME_CODE_SEQUENCE: Tag = Tag(0x0040, 0xA043);
    const CONCEPT_CODE_SEQUENCE: Tag = Tag(0x0040, 0xA168);

    let value_type = read_item_string(item, VALUE_TYPE).map(|value| value.to_ascii_uppercase());

    SrIndexedNode {
        path: path.clone(),
        relationship_type: read_item_string(item, RELATIONSHIP_TYPE),
        referenced_content_item_identifier: read_item_multi_int(
            item,
            REFERENCED_CONTENT_ITEM_IDENTIFIER,
        )
        .map(|parts| {
            parts
                .into_iter()
                .filter_map(|part| usize::try_from(part).ok())
                .filter(|part| *part > 0)
                .collect::<Vec<_>>()
        })
        .filter(|parts| !parts.is_empty()),
        concept_name: read_code_from_item(item, CONCEPT_NAME_CODE_SEQUENCE),
        coded_value: read_code_from_item(item, CONCEPT_CODE_SEQUENCE),
        numeric_value: parse_numeric_value(item),
        image_reference: value_type
            .as_deref()
            .filter(|value| *value == "IMAGE")
            .and_then(|_| referenced_image_target_from_sr_item(item)),
        spatial_coordinates: value_type
            .as_deref()
            .filter(|value| *value == "SCOORD")
            .and_then(|_| parse_spatial_coordinates(item)),
        children: sequence_items_from_item(item, CONTENT_SEQUENCE)
            .map(|children| build_indexed_nodes(children, &path))
            .unwrap_or_default(),
    }
}

fn collect_image_references(
    nodes: &[SrIndexedNode],
    image_index: &mut HashMap<Vec<usize>, ReferencedImageTarget>,
) {
    for node in nodes {
        if let Some(mut reference) = node.image_reference.clone() {
            hydrate_image_context(&mut reference, node);
            image_index.insert(node.path.clone(), reference);
        }
        collect_image_references(&node.children, image_index);
    }
}

fn hydrate_image_context(target: &mut ReferencedImageTarget, node: &SrIndexedNode) {
    target.laterality = node.acquisition_context_value("Image Laterality");
    target.view = node.acquisition_context_value("Image View");
}

fn collect_mammography_cad_overlays(
    nodes: &[SrIndexedNode],
    image_index: &HashMap<Vec<usize>, ReferencedImageTarget>,
    overlays: &mut HashMap<String, SrOverlay>,
) {
    for node in nodes {
        if node.is_feature_or_finding() {
            let Some(rendering_intent) = node.rendering_intent() else {
                collect_mammography_cad_overlays(&node.children, image_index, overlays);
                continue;
            };
            if rendering_intent == SrRenderingIntent::NotForPresentation {
                collect_mammography_cad_overlays(&node.children, image_index, overlays);
                continue;
            }

            let cad_operating_point = node.cad_operating_point();
            let mut geometry_nodes = Vec::new();
            node.property_geometry_nodes(&mut geometry_nodes);
            let mut labeled_targets = Vec::<ReferencedImageTarget>::new();

            for geometry_node in geometry_nodes.iter().copied() {
                let Some(reference) = geometry_node.selected_image_target(image_index) else {
                    continue;
                };
                let Some(spatial_coordinates) = geometry_node.spatial_coordinates.as_ref() else {
                    continue;
                };
                let graphics = graphics_from_spatial_coordinates(spatial_coordinates);
                if graphics.is_empty() {
                    continue;
                }

                let overlay = overlays
                    .entry(reference.sop_instance_uid.clone())
                    .or_default();
                overlay
                    .graphics
                    .extend(graphics.into_iter().map(|graphic| SrOverlayGraphic {
                        graphic,
                        referenced_frames: reference.referenced_frames.clone(),
                        rendering_intent,
                        cad_operating_point,
                    }));

                if labeled_targets.contains(&reference) {
                    continue;
                }

                let Some((anchor, units)) =
                    preferred_sr_overlay_label_anchor(&geometry_nodes, image_index, &reference)
                else {
                    continue;
                };
                let lines = sr_overlay_label_lines(node, &reference);
                if lines.is_empty() {
                    continue;
                }

                overlay.labels.push(SrOverlayLabel {
                    anchor,
                    units,
                    lines,
                    referenced_frames: reference.referenced_frames.clone(),
                    rendering_intent,
                });
                labeled_targets.push(reference);
            }
        }

        collect_mammography_cad_overlays(&node.children, image_index, overlays);
    }
}

fn relationship_type_matches(relationship_type: Option<&str>, expected: &str) -> bool {
    relationship_type.is_some_and(|value| value.eq_ignore_ascii_case(expected))
}

fn referenced_image_target_from_sr_item(item: &InMemDicomObject) -> Option<ReferencedImageTarget> {
    const REFERENCED_SOP_SEQUENCE: Tag = Tag(0x0008, 0x1199);
    const REFERENCED_SOP_INSTANCE_UID: Tag = Tag(0x0008, 0x1155);
    const REFERENCED_FRAME_NUMBER: Tag = Tag(0x0008, 0x1160);

    let reference_item =
        sequence_items_from_item(item, REFERENCED_SOP_SEQUENCE).and_then(|items| items.first())?;
    let sop_instance_uid = read_item_string(reference_item, REFERENCED_SOP_INSTANCE_UID)?;
    let referenced_frames =
        read_item_multi_int(reference_item, REFERENCED_FRAME_NUMBER).map(|frames| {
            let mut frames = frames
                .into_iter()
                .filter_map(|frame| usize::try_from(frame).ok())
                .filter(|frame| *frame > 0)
                .collect::<Vec<_>>();
            frames.sort_unstable();
            frames.dedup();
            frames
        });

    Some(ReferencedImageTarget {
        sop_instance_uid,
        referenced_frames,
        laterality: None,
        view: None,
    })
}

fn read_graphic_annotation_units(item: &InMemDicomObject) -> GspsUnits {
    const GRAPHIC_ANNOTATION_UNITS: Tag = Tag(0x0070, 0x0005);

    match read_item_string(item, GRAPHIC_ANNOTATION_UNITS)
        .map(|value| value.to_ascii_uppercase())
        .as_deref()
    {
        Some("DISPLAY") => GspsUnits::Display,
        _ => GspsUnits::Pixel,
    }
}

fn parse_spatial_coordinates(item: &InMemDicomObject) -> Option<SrSpatialCoordinates> {
    const GRAPHIC_DATA: Tag = Tag(0x0070, 0x0022);
    const GRAPHIC_TYPE: Tag = Tag(0x0070, 0x0023);

    let points = read_item_multi_float(item, GRAPHIC_DATA)
        .map(parse_graphic_points)
        .unwrap_or_default();
    if points.is_empty() {
        return None;
    }

    Some(SrSpatialCoordinates {
        graphic_type: read_item_string(item, GRAPHIC_TYPE)
            .unwrap_or_else(|| "POLYLINE".to_string())
            .to_ascii_uppercase(),
        points,
        units: read_graphic_annotation_units(item),
    })
}

fn graphics_from_spatial_coordinates(
    spatial_coordinates: &SrSpatialCoordinates,
) -> Vec<GspsGraphic> {
    let units = spatial_coordinates.units;

    match spatial_coordinates.graphic_type.as_str() {
        "POINT" | "MULTIPOINT" => spatial_coordinates
            .points
            .iter()
            .map(|(x, y)| GspsGraphic::Point {
                x: *x,
                y: *y,
                units,
            })
            .collect(),
        "CIRCLE" if spatial_coordinates.points.len() >= 2 => {
            let polyline =
                approximate_circle(spatial_coordinates.points[0], spatial_coordinates.points[1]);
            vec![GspsGraphic::Polyline {
                points: polyline,
                units,
                closed: true,
            }]
        }
        "ELLIPSE" if spatial_coordinates.points.len() >= 4 => {
            let polyline = approximate_ellipse(
                spatial_coordinates.points[0],
                spatial_coordinates.points[1],
                spatial_coordinates.points[2],
                spatial_coordinates.points[3],
            );
            vec![GspsGraphic::Polyline {
                points: polyline,
                units,
                closed: true,
            }]
        }
        "POLYLINE" => vec![GspsGraphic::Polyline {
            points: spatial_coordinates.points.clone(),
            units,
            closed: false,
        }],
        _ => Vec::new(),
    }
}

fn parse_graphic_points(values: Vec<f32>) -> Vec<(f32, f32)> {
    let mut points = Vec::with_capacity(values.len() / 2);
    for pair in values.chunks_exact(2) {
        points.push((pair[0], pair[1]));
    }
    points
}

fn approximate_circle(center: (f32, f32), perimeter: (f32, f32)) -> Vec<(f32, f32)> {
    const STEPS: usize = 64;
    let radius = ((perimeter.0 - center.0).powi(2) + (perimeter.1 - center.1).powi(2)).sqrt();
    if radius <= f32::EPSILON {
        return vec![center];
    }

    (0..STEPS)
        .map(|index| {
            let t = 2.0_f32 * std::f32::consts::PI * (index as f32 / STEPS as f32);
            (center.0 + radius * t.cos(), center.1 + radius * t.sin())
        })
        .collect()
}

fn approximate_ellipse(
    major_start: (f32, f32),
    major_end: (f32, f32),
    minor_start: (f32, f32),
    minor_end: (f32, f32),
) -> Vec<(f32, f32)> {
    const STEPS: usize = 64;
    let center_x = (major_start.0 + major_end.0 + minor_start.0 + minor_end.0) * 0.25;
    let center_y = (major_start.1 + major_end.1 + minor_start.1 + minor_end.1) * 0.25;
    let major_vector = (
        (major_end.0 - major_start.0) * 0.5,
        (major_end.1 - major_start.1) * 0.5,
    );
    let minor_vector = (
        (minor_end.0 - minor_start.0) * 0.5,
        (minor_end.1 - minor_start.1) * 0.5,
    );

    let major_len = (major_vector.0.powi(2) + major_vector.1.powi(2)).sqrt();
    let minor_len = (minor_vector.0.powi(2) + minor_vector.1.powi(2)).sqrt();
    if major_len <= f32::EPSILON || minor_len <= f32::EPSILON {
        return vec![(center_x, center_y)];
    }

    (0..STEPS)
        .map(|index| {
            let t = 2.0_f32 * std::f32::consts::PI * (index as f32 / STEPS as f32);
            (
                center_x + major_vector.0 * t.cos() + minor_vector.0 * t.sin(),
                center_y + major_vector.1 * t.cos() + minor_vector.1 * t.sin(),
            )
        })
        .collect()
}

fn parse_numeric_value(item: &InMemDicomObject) -> Option<f32> {
    const MEASURED_VALUE_SEQUENCE: Tag = Tag(0x0040, 0xA300);
    const NUMERIC_VALUE: Tag = Tag(0x0040, 0xA30A);

    let measured_item =
        sequence_items_from_item(item, MEASURED_VALUE_SEQUENCE).and_then(|items| items.first())?;
    read_item_string(measured_item, NUMERIC_VALUE)?
        .parse::<f32>()
        .ok()
}

fn read_code_from_item(item: &InMemDicomObject, tag: Tag) -> SrCode {
    let Some(code_item) = sequence_items_from_item(item, tag).and_then(|items| items.first())
    else {
        return SrCode::default();
    };

    SrCode {
        value: read_item_string(code_item, Tag(0x0008, 0x0100)),
        meaning: read_item_string(code_item, Tag(0x0008, 0x0104)),
    }
}

fn code_display(code: &SrCode) -> Option<&str> {
    code.meaning.as_deref().or(code.value.as_deref())
}

fn sr_overlay_label_anchor(node: &SrIndexedNode) -> Option<((f32, f32), GspsUnits)> {
    let spatial_coordinates = node.spatial_coordinates.as_ref()?;
    if spatial_coordinates.points.is_empty() {
        return None;
    }

    if spatial_coordinates.graphic_type == "POINT"
        || spatial_coordinates.graphic_type == "MULTIPOINT"
    {
        return spatial_coordinates
            .points
            .first()
            .copied()
            .map(|anchor| (anchor, spatial_coordinates.units));
    }

    let min_x = spatial_coordinates
        .points
        .iter()
        .map(|(x, _)| *x)
        .fold(f32::INFINITY, f32::min);
    let min_y = spatial_coordinates
        .points
        .iter()
        .map(|(_, y)| *y)
        .fold(f32::INFINITY, f32::min);
    (min_x.is_finite() && min_y.is_finite()).then_some(((min_x, min_y), spatial_coordinates.units))
}

fn preferred_sr_overlay_label_anchor(
    geometry_nodes: &[&SrIndexedNode],
    image_index: &HashMap<Vec<usize>, ReferencedImageTarget>,
    reference: &ReferencedImageTarget,
) -> Option<((f32, f32), GspsUnits)> {
    geometry_nodes
        .iter()
        .copied()
        .filter(|node| {
            node.selected_image_target(image_index)
                .is_some_and(|target| target == *reference)
        })
        .find(|node| node.concept_name.matches_meaning("Center"))
        .and_then(sr_overlay_label_anchor)
        .or_else(|| {
            geometry_nodes
                .iter()
                .copied()
                .filter(|node| {
                    node.selected_image_target(image_index)
                        .is_some_and(|target| target == *reference)
                })
                .find_map(sr_overlay_label_anchor)
        })
}

fn sr_overlay_label_lines(
    finding_node: &SrIndexedNode,
    reference: &ReferencedImageTarget,
) -> Vec<String> {
    let mut lines = Vec::new();

    if let Some(finding) = code_display(&finding_node.coded_value) {
        lines.push(finding.to_string());
    }

    let mut context_parts = Vec::new();
    if let Some(context) = sr_overlay_context_label(reference) {
        context_parts.push(context);
    }
    if let Some(certainty) = finding_node.certainty_of_finding() {
        context_parts.push(format!("{}%", format_sr_overlay_number(certainty)));
    }
    if !context_parts.is_empty() {
        lines.push(context_parts.join(" | "));
    }

    lines
}

fn sr_overlay_context_label(reference: &ReferencedImageTarget) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(laterality) = reference
        .laterality
        .as_deref()
        .and_then(mammography_laterality_abbreviation)
    {
        parts.push(laterality);
    }
    if let Some(view) = reference
        .view
        .as_deref()
        .and_then(mammography_view_abbreviation)
    {
        parts.push(view);
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

fn mammography_laterality_abbreviation(value: &str) -> Option<&'static str> {
    match normalize_code_token(value).as_str() {
        "LEFTBREAST" => Some("L"),
        "RIGHTBREAST" => Some("R"),
        "BILATERALBREASTS" | "BILATERAL" => Some("B"),
        _ => None,
    }
}

fn mammography_view_abbreviation(value: &str) -> Option<&'static str> {
    match normalize_code_token(value).as_str() {
        "CRANIOCAUDAL" => Some("CC"),
        "MEDIOLATERALOBLIQUE" => Some("MLO"),
        "MEDIOLATERAL" => Some("ML"),
        "LATEROMEDIAL" => Some("LM"),
        "EXAGGERATEDCRANIOCAUDALLATERAL" => Some("XCCL"),
        "EXAGGERATEDCRANIOCAUDALMEDIAL" => Some("XCCM"),
        _ => None,
    }
}

fn format_sr_overlay_number(value: f32) -> String {
    if (value.fract()).abs() < 0.01 {
        format!("{value:.0}")
    } else {
        let output = format!("{value:.1}");
        output
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    }
}

fn normalize_code_token(value: &str) -> String {
    value
        .trim()
        .to_ascii_uppercase()
        .replace([' ', '-', '_'], "")
}

fn parse_structured_report_nodes(items: &[InMemDicomObject]) -> Vec<StructuredReportNode> {
    items.iter().map(parse_structured_report_node).collect()
}

fn parse_structured_report_node(item: &InMemDicomObject) -> StructuredReportNode {
    const CONTENT_SEQUENCE: Tag = Tag(0x0040, 0xA730);
    const RELATIONSHIP_TYPE: Tag = Tag(0x0040, 0xA010);
    const VALUE_TYPE: Tag = Tag(0x0040, 0xA040);
    const CONCEPT_NAME_CODE_SEQUENCE: Tag = Tag(0x0040, 0xA043);

    let value_type = read_item_string(item, VALUE_TYPE)
        .unwrap_or_else(|| "CONTAINER".to_string())
        .to_ascii_uppercase();
    let label = read_code_sequence_display_from_item(item, CONCEPT_NAME_CODE_SEQUENCE)
        .unwrap_or_else(|| default_sr_label(&value_type).to_string());

    StructuredReportNode {
        relationship_type: read_item_string(item, RELATIONSHIP_TYPE),
        label,
        value: structured_report_value(item, &value_type),
        children: sequence_items_from_item(item, CONTENT_SEQUENCE)
            .map(parse_structured_report_nodes)
            .unwrap_or_default(),
    }
}

fn structured_report_value(item: &InMemDicomObject, value_type: &str) -> Option<String> {
    const TEXT_VALUE: Tag = Tag(0x0040, 0xA160);
    const DATE_TIME: Tag = Tag(0x0040, 0xA120);
    const DATE: Tag = Tag(0x0040, 0xA121);
    const TIME: Tag = Tag(0x0040, 0xA122);
    const PERSON_NAME: Tag = Tag(0x0040, 0xA123);
    const UIDREF: Tag = Tag(0x0040, 0xA124);
    const CONCEPT_CODE_SEQUENCE: Tag = Tag(0x0040, 0xA168);
    const MEASURED_VALUE_SEQUENCE: Tag = Tag(0x0040, 0xA300);
    const NUMERIC_VALUE: Tag = Tag(0x0040, 0xA30A);
    const MEASUREMENT_UNITS_CODE_SEQUENCE: Tag = Tag(0x0040, 0x08EA);
    const TEMPORAL_RANGE_TYPE: Tag = Tag(0x0040, 0xA130);
    const GRAPHIC_TYPE: Tag = Tag(0x0070, 0x0023);

    match value_type {
        "TEXT" => read_item_string(item, TEXT_VALUE),
        "CODE" => read_code_sequence_display_from_item(item, CONCEPT_CODE_SEQUENCE),
        "NUM" => {
            let measured_item = sequence_items_from_item(item, MEASURED_VALUE_SEQUENCE)
                .and_then(|items| items.first())?;
            let numeric_value = read_item_string(measured_item, NUMERIC_VALUE)?;
            let units = read_code_sequence_display_from_item(
                measured_item,
                MEASUREMENT_UNITS_CODE_SEQUENCE,
            );
            Some(match units {
                Some(units) => format!("{numeric_value} {units}"),
                None => numeric_value,
            })
        }
        "DATETIME" => read_item_string(item, DATE_TIME),
        "DATE" => read_item_string(item, DATE),
        "TIME" => read_item_string(item, TIME),
        "PNAME" => read_item_string(item, PERSON_NAME),
        "UIDREF" => read_item_string(item, UIDREF),
        "IMAGE" | "COMPOSITE" | "WAVEFORM" => structured_report_reference_value(item),
        "TCOORD" => read_item_string(item, TEMPORAL_RANGE_TYPE),
        "SCOORD" | "SCOORD3D" => read_item_string(item, GRAPHIC_TYPE),
        _ => None,
    }
}

fn structured_report_reference_value(item: &InMemDicomObject) -> Option<String> {
    const REFERENCED_SOP_SEQUENCE: Tag = Tag(0x0008, 0x1199);
    const REFERENCED_SOP_CLASS_UID: Tag = Tag(0x0008, 0x1150);
    const REFERENCED_SOP_INSTANCE_UID: Tag = Tag(0x0008, 0x1155);

    let reference_item =
        sequence_items_from_item(item, REFERENCED_SOP_SEQUENCE).and_then(|items| items.first())?;
    let instance_uid = read_item_string(reference_item, REFERENCED_SOP_INSTANCE_UID)?;
    let sop_class = read_item_string(reference_item, REFERENCED_SOP_CLASS_UID);

    Some(match sop_class {
        Some(sop_class) => format!("{instance_uid} ({sop_class})"),
        None => instance_uid,
    })
}

fn read_code_sequence_display_from_item(item: &InMemDicomObject, tag: Tag) -> Option<String> {
    let code_item = sequence_items_from_item(item, tag).and_then(|items| items.first())?;
    read_item_string(code_item, Tag(0x0008, 0x0104))
        .or_else(|| read_item_string(code_item, Tag(0x0008, 0x0100)))
        .or_else(|| read_item_string(code_item, Tag(0x0008, 0x0102)))
}

fn default_sr_label(value_type: &str) -> &'static str {
    match value_type {
        "CODE" => "Code",
        "COMPOSITE" => "Referenced Object",
        "CONTAINER" => "Container",
        "DATE" => "Date",
        "DATETIME" => "Date/Time",
        "IMAGE" => "Referenced Image",
        "NUM" => "Numeric Value",
        "PNAME" => "Person Name",
        "SCOORD" => "Spatial Coordinates",
        "SCOORD3D" => "3D Spatial Coordinates",
        "TCOORD" => "Temporal Coordinates",
        "TEXT" => "Text",
        "TIME" => "Time",
        "UIDREF" => "UID Reference",
        "WAVEFORM" => "Referenced Waveform",
        _ => "Structured Report Item",
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::dicom::{
        BASIC_TEXT_SR_SOP_CLASS_UID, DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID,
        EXPLICIT_VR_LITTLE_ENDIAN_UID, MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
        STRUCTURED_REPORT_SOP_CLASS_UID_PREFIX,
    };
    use dicom_core::value::DataSetSequence;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_object::{from_reader, FileMetaTableBuilder};

    fn custom_code_item(value: &str, scheme: &str, meaning: &str) -> InMemDicomObject {
        InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0100), VR::SH, value),
            DataElement::new(Tag(0x0008, 0x0102), VR::SH, scheme),
            DataElement::new(Tag(0x0008, 0x0104), VR::LO, meaning),
        ])
    }

    fn code_item(meaning: &str) -> InMemDicomObject {
        let value = meaning.replace(' ', "_");
        custom_code_item(&value, "99TEST", meaning)
    }

    fn image_reference_item(
        sop_instance_uid: &str,
        referenced_frames: Option<&[i32]>,
    ) -> InMemDicomObject {
        let mut referenced = InMemDicomObject::new_empty();
        referenced.put(DataElement::new(
            Tag(0x0008, 0x1150),
            VR::UI,
            DIGITAL_MAMMOGRAPHY_XRAY_IMAGE_PRESENTATION_SOP_CLASS_UID,
        ));
        referenced.put(DataElement::new(
            Tag(0x0008, 0x1155),
            VR::UI,
            sop_instance_uid,
        ));
        if let Some(referenced_frames) = referenced_frames {
            referenced.put(DataElement::new(
                Tag(0x0008, 0x1160),
                VR::IS,
                referenced_frames
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join("\\"),
            ));
        }

        InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0040, 0xA040), VR::CS, "IMAGE"),
            DataElement::new(
                Tag(0x0008, 0x1199),
                VR::SQ,
                DataSetSequence::from(vec![referenced]),
            ),
        ])
    }

    fn image_library_reference_item(
        sop_instance_uid: &str,
        referenced_frames: Option<&[i32]>,
        laterality: &str,
        view: &str,
    ) -> InMemDicomObject {
        let mut item = image_reference_item(sop_instance_uid, referenced_frames);
        item.put(DataElement::new(
            Tag(0x0040, 0xA730),
            VR::SQ,
            DataSetSequence::from(vec![
                code_content_item(Some("HAS ACQ CONTEXT"), "Image Laterality", laterality),
                code_content_item(Some("HAS ACQ CONTEXT"), "Image View", view),
            ]),
        ));
        item
    }

    fn referenced_item_identifier_item(path: &[usize]) -> InMemDicomObject {
        InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0040, 0xDB73),
            VR::UL,
            PrimitiveValue::U32(path.iter().map(|part| *part as u32).collect()),
        )])
    }

    fn content_item(
        value_type: &str,
        relationship_type: Option<&str>,
        concept_name: Option<&str>,
        value_elements: Vec<DataElement<InMemDicomObject>>,
        children: Vec<InMemDicomObject>,
    ) -> InMemDicomObject {
        let mut item = InMemDicomObject::new_empty();
        item.put(DataElement::new(Tag(0x0040, 0xA040), VR::CS, value_type));
        if let Some(relationship_type) = relationship_type {
            item.put(DataElement::new(
                Tag(0x0040, 0xA010),
                VR::CS,
                relationship_type,
            ));
        }
        if let Some(concept_name) = concept_name {
            item.put(DataElement::new(
                Tag(0x0040, 0xA043),
                VR::SQ,
                DataSetSequence::from(vec![code_item(concept_name)]),
            ));
        }
        for value_element in value_elements {
            item.put(value_element);
        }
        if !children.is_empty() {
            item.put(DataElement::new(
                Tag(0x0040, 0xA730),
                VR::SQ,
                DataSetSequence::from(children),
            ));
        }
        item
    }

    fn code_content_item(
        relationship_type: Option<&str>,
        concept_name: &str,
        coded_value: &str,
    ) -> InMemDicomObject {
        content_item(
            "CODE",
            relationship_type,
            Some(concept_name),
            vec![DataElement::new(
                Tag(0x0040, 0xA168),
                VR::SQ,
                DataSetSequence::from(vec![code_item(coded_value)]),
            )],
            Vec::new(),
        )
    }

    fn numeric_content_item(
        relationship_type: Option<&str>,
        concept_name: &str,
        numeric_value: &str,
    ) -> InMemDicomObject {
        let measured_value = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0040, 0xA30A),
            VR::DS,
            numeric_value,
        )]);
        content_item(
            "NUM",
            relationship_type,
            Some(concept_name),
            vec![DataElement::new(
                Tag(0x0040, 0xA300),
                VR::SQ,
                DataSetSequence::from(vec![measured_value]),
            )],
            Vec::new(),
        )
    }

    fn scoord_content_item_with_units(
        relationship_type: Option<&str>,
        concept_name: &str,
        graphic_type: &str,
        graphic_data: &[f32],
        referenced_item_path: &[usize],
        units: Option<&str>,
    ) -> InMemDicomObject {
        let mut value_elements = Vec::new();
        if let Some(units) = units {
            value_elements.push(DataElement::new(Tag(0x0070, 0x0005), VR::CS, units));
        }
        value_elements.extend([
            DataElement::new(Tag(0x0070, 0x0023), VR::CS, graphic_type),
            DataElement::new(
                Tag(0x0070, 0x0022),
                VR::FL,
                PrimitiveValue::F32(graphic_data.iter().copied().collect()),
            ),
        ]);
        content_item(
            "SCOORD",
            relationship_type,
            Some(concept_name),
            value_elements,
            vec![referenced_item_identifier_item(referenced_item_path)],
        )
    }

    fn inline_scoord_content_item(
        relationship_type: Option<&str>,
        concept_name: &str,
        graphic_type: &str,
        graphic_data: &[f32],
        image_reference: InMemDicomObject,
    ) -> InMemDicomObject {
        content_item(
            "SCOORD",
            relationship_type,
            Some(concept_name),
            vec![
                DataElement::new(Tag(0x0070, 0x0023), VR::CS, graphic_type),
                DataElement::new(
                    Tag(0x0070, 0x0022),
                    VR::FL,
                    PrimitiveValue::F32(graphic_data.iter().copied().collect()),
                ),
            ],
            vec![image_reference],
        )
    }

    fn mammography_cad_sr_object(
        rendering_intent: &str,
        referenced_frames: Option<&[i32]>,
        sop_class_uid: &str,
    ) -> DefaultDicomObject {
        mammography_cad_sr_object_with_context(
            rendering_intent,
            referenced_frames,
            sop_class_uid,
            "test-side-omega",
            "test-view-sigma",
            None,
        )
    }

    fn mammography_cad_sr_object_with_context(
        rendering_intent: &str,
        referenced_frames: Option<&[i32]>,
        sop_class_uid: &str,
        laterality: &str,
        view: &str,
        graphic_units: Option<&str>,
    ) -> DefaultDicomObject {
        let image_library = content_item(
            "CONTAINER",
            None,
            Some("Image Library"),
            Vec::new(),
            vec![image_library_reference_item(
                "1.2.3.4",
                referenced_frames,
                laterality,
                view,
            )],
        );
        let finding = content_item(
            "CODE",
            Some("CONTAINS"),
            Some("Single Image Finding"),
            vec![DataElement::new(
                Tag(0x0040, 0xA168),
                VR::SQ,
                DataSetSequence::from(vec![code_item("TEST-FINDING-ALPHA")]),
            )],
            vec![
                code_content_item(
                    Some("HAS CONCEPT MOD"),
                    "Rendering Intent",
                    rendering_intent,
                ),
                numeric_content_item(Some("HAS CONCEPT MOD"), "CAD Operating Point", "2"),
                numeric_content_item(Some("HAS PROPERTIES"), "Certainty of Finding", "1234.5"),
                scoord_content_item_with_units(
                    Some("HAS PROPERTIES"),
                    "Center",
                    "POINT",
                    &[16.0, 24.0],
                    &[1, 1, 1],
                    graphic_units,
                ),
                scoord_content_item_with_units(
                    Some("HAS PROPERTIES"),
                    "Outline",
                    "POLYLINE",
                    &[10.0, 20.0, 20.0, 30.0, 30.0, 20.0],
                    &[1, 1, 1],
                    graphic_units,
                ),
            ],
        );

        InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, sop_class_uid),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
            DataElement::new(
                Tag(0x0040, 0xA730),
                VR::SQ,
                DataSetSequence::from(vec![image_library, finding]),
            ),
        ])
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                .media_storage_sop_class_uid(sop_class_uid)
                .media_storage_sop_instance_uid("9.8.7.6"),
        )
        .expect("mammography CAD SR test object should build file meta")
    }

    fn mammography_cad_sr_object_with_inline_image_context(
        rendering_intent: &str,
        referenced_frames: Option<&[i32]>,
        sop_class_uid: &str,
        laterality: &str,
        view: &str,
    ) -> DefaultDicomObject {
        let finding = content_item(
            "CODE",
            Some("CONTAINS"),
            Some("Single Image Finding"),
            vec![DataElement::new(
                Tag(0x0040, 0xA168),
                VR::SQ,
                DataSetSequence::from(vec![code_item("TEST-FINDING-ALPHA")]),
            )],
            vec![
                code_content_item(
                    Some("HAS CONCEPT MOD"),
                    "Rendering Intent",
                    rendering_intent,
                ),
                numeric_content_item(Some("HAS CONCEPT MOD"), "CAD Operating Point", "2"),
                numeric_content_item(Some("HAS PROPERTIES"), "Certainty of Finding", "1234.5"),
                inline_scoord_content_item(
                    Some("HAS PROPERTIES"),
                    "Center",
                    "POINT",
                    &[16.0, 24.0],
                    image_library_reference_item("1.2.3.4", referenced_frames, laterality, view),
                ),
                inline_scoord_content_item(
                    Some("HAS PROPERTIES"),
                    "Outline",
                    "POLYLINE",
                    &[10.0, 20.0, 20.0, 30.0, 30.0, 20.0],
                    image_library_reference_item("1.2.3.4", referenced_frames, laterality, view),
                ),
            ],
        );

        InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, sop_class_uid),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
            DataElement::new(
                Tag(0x0040, 0xA730),
                VR::SQ,
                DataSetSequence::from(vec![finding]),
            ),
        ])
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                .media_storage_sop_class_uid(sop_class_uid)
                .media_storage_sop_instance_uid("9.8.7.10"),
        )
        .expect("inline-image mammography CAD SR test object should build file meta")
    }

    fn simple_structured_report_object() -> DefaultDicomObject {
        InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, BASIC_TEXT_SR_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0060), VR::CS, "SR"),
            DataElement::new(Tag(0x0008, 0x103E), VR::LO, "Chest SR"),
            DataElement::new(Tag(0x0040, 0xA491), VR::CS, "COMPLETE"),
            DataElement::new(Tag(0x0040, 0xA493), VR::CS, "UNVERIFIED"),
        ])
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                .media_storage_sop_class_uid(BASIC_TEXT_SR_SOP_CLASS_UID)
                .media_storage_sop_instance_uid("9.8.7.5"),
        )
        .expect("simple structured report test object should build file meta")
    }

    fn roundtrip_object_bytes(object: &DefaultDicomObject) -> DefaultDicomObject {
        let mut bytes = Vec::new();
        object
            .write_all(&mut bytes)
            .expect("test object should serialize");
        from_reader(Cursor::new(bytes)).expect("serialized test object should deserialize")
    }

    #[test]
    fn parse_structured_report_document_populates_full_metadata() {
        let sr_obj = simple_structured_report_object();

        let report = parse_structured_report_document(&sr_obj);

        assert!(report
            .full_metadata
            .iter()
            .any(|field| field.keyword == "SeriesDescription"));
        assert!(report
            .full_metadata
            .iter()
            .any(|field| field.keyword == "CompletionFlag"));
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_extracts_required_geometry() {
        let sr_obj = mammography_cad_sr_object(
            "Presentation Required",
            Some(&[2]),
            MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
        );

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);
        let overlay = overlays
            .get("1.2.3.4")
            .expect("overlay should resolve image library reference");

        assert_eq!(overlay.graphics.len(), 2);
        assert_eq!(overlay.labels.len(), 1);
        assert_eq!(overlay.graphics[0].referenced_frames, Some(vec![2]));
        assert_eq!(overlay.visible_graphics_for_frame(1).count(), 2);
        assert_eq!(overlay.visible_labels_for_frame(1).count(), 1);
        assert_eq!(
            overlay.graphics[0].rendering_intent,
            SrRenderingIntent::PresentationRequired
        );
        assert_eq!(overlay.graphics[0].cad_operating_point, Some(2.0));
        assert_eq!(
            overlay.labels[0].lines,
            vec!["TEST-FINDING-ALPHA".to_string(), "1234.5%".to_string()]
        );
        assert_eq!(overlay.labels[0].anchor, (16.0, 24.0));
        assert_eq!(overlay.labels[0].units, GspsUnits::Pixel);
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_formats_full_mammography_context_label() {
        let sr_obj = mammography_cad_sr_object_with_context(
            "Presentation Required",
            None,
            MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
            "Left Breast",
            "Cranio-caudal",
            None,
        );

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);
        let overlay = overlays
            .get("1.2.3.4")
            .expect("overlay should resolve image library reference");

        assert_eq!(
            overlay.labels[0].lines,
            vec![
                "TEST-FINDING-ALPHA".to_string(),
                "L CC | 1234.5%".to_string()
            ]
        );
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_hydrates_inline_image_context() {
        let sr_obj = mammography_cad_sr_object_with_inline_image_context(
            "Presentation Required",
            None,
            MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
            "Left Breast",
            "Cranio-caudal",
        );

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);
        let overlay = overlays
            .get("1.2.3.4")
            .expect("overlay should resolve inline image reference");

        assert_eq!(
            overlay.labels[0].lines,
            vec![
                "TEST-FINDING-ALPHA".to_string(),
                "L CC | 1234.5%".to_string()
            ]
        );
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_preserves_display_units_for_geometry_and_labels() {
        let sr_obj = mammography_cad_sr_object_with_context(
            "Presentation Required",
            None,
            MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
            "Left Breast",
            "Cranio-caudal",
            Some("DISPLAY"),
        );

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);
        let overlay = overlays
            .get("1.2.3.4")
            .expect("overlay should resolve image library reference");

        for graphic in &overlay.graphics {
            match &graphic.graphic {
                GspsGraphic::Point { units, .. } | GspsGraphic::Polyline { units, .. } => {
                    assert_eq!(*units, GspsUnits::Display);
                }
            }
        }
        assert_eq!(overlay.labels[0].anchor, (16.0, 24.0));
        assert_eq!(overlay.labels[0].units, GspsUnits::Display);
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_preserves_ul_content_item_references_after_roundtrip() {
        let sr_obj = roundtrip_object_bytes(&mammography_cad_sr_object(
            "Presentation Required",
            Some(&[2]),
            MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
        ));

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);
        let overlay = overlays
            .get("1.2.3.4")
            .expect("overlay should resolve serialized UL referenced content item path");

        assert_eq!(overlay.graphics.len(), 2);
        assert_eq!(overlay.labels.len(), 1);
        assert_eq!(overlay.visible_graphics_for_frame(1).count(), 2);
        assert_eq!(overlay.visible_labels_for_frame(1).count(), 1);
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_keeps_optional_marks_hidden_in_v1() {
        let sr_obj = mammography_cad_sr_object(
            "Presentation Optional",
            None,
            MAMMOGRAPHY_CAD_SR_SOP_CLASS_UID,
        );

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);
        let overlay = overlays
            .get("1.2.3.4")
            .expect("optional overlay should still be preserved");

        assert_eq!(overlay.graphics.len(), 2);
        assert_eq!(overlay.labels.len(), 1);
        assert_eq!(overlay.visible_graphics_for_frame(0).count(), 0);
        assert_eq!(overlay.visible_labels_for_frame(0).count(), 0);
        assert!(overlay
            .graphics
            .iter()
            .all(|graphic| graphic.rendering_intent == SrRenderingIntent::PresentationOptional));
    }

    #[test]
    fn parse_mammography_cad_sr_overlays_ignores_non_mammo_cad_sr() {
        let non_mammo_uid = format!("{STRUCTURED_REPORT_SOP_CLASS_UID_PREFIX}11");
        let sr_obj = mammography_cad_sr_object("Presentation Required", None, &non_mammo_uid);

        let overlays = parse_mammography_cad_sr_overlays(&sr_obj);

        assert!(overlays.is_empty());
    }
}
