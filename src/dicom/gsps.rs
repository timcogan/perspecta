use std::collections::HashMap;

use anyhow::{bail, Result};
use dicom_object::{DefaultDicomObject, InMemDicomObject, Tag};

use super::{
    classify_dicom_object, read_item_multi_float, read_item_multi_int, read_item_string,
    read_string, sequence_items_from_item, sequence_items_from_object, DicomPathKind, DicomSource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GspsUnits {
    Pixel,
    Display,
}

#[derive(Debug, Clone)]
pub enum GspsGraphic {
    Point {
        x: f32,
        y: f32,
        units: GspsUnits,
    },
    Polyline {
        points: Vec<(f32, f32)>,
        units: GspsUnits,
        closed: bool,
    },
}

#[derive(Debug, Clone)]
pub struct GspsOverlayGraphic {
    pub graphic: GspsGraphic,
    pub referenced_frames: Option<Vec<usize>>,
}

#[derive(Debug, Clone, Default)]
pub struct GspsOverlay {
    pub graphics: Vec<GspsOverlayGraphic>,
}

impl GspsOverlay {
    pub fn is_empty(&self) -> bool {
        self.graphics.is_empty()
    }

    #[cfg(test)]
    pub fn from_graphics(graphics: Vec<GspsGraphic>) -> Self {
        Self {
            graphics: graphics
                .into_iter()
                .map(|graphic| GspsOverlayGraphic {
                    graphic,
                    referenced_frames: None,
                })
                .collect(),
        }
    }

    pub fn graphics_for_frame(
        &self,
        frame_index: usize,
    ) -> impl Iterator<Item = &GspsGraphic> + '_ {
        let dicom_frame_number = frame_index.saturating_add(1);
        self.graphics.iter().filter_map(move |graphic| {
            let applies = match graphic.referenced_frames.as_ref() {
                Some(frames) => frames.contains(&dicom_frame_number),
                None => true,
            };
            applies.then_some(&graphic.graphic)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReferencedImageTarget {
    sop_instance_uid: String,
    referenced_frames: Option<Vec<usize>>,
}

pub fn load_gsps_overlays(source: impl Into<DicomSource>) -> Result<HashMap<String, GspsOverlay>> {
    let source = source.into();
    let obj = super::open_dicom_object(&source)?;
    if classify_dicom_object(&obj) != DicomPathKind::Gsps {
        let sop_class = read_string(&obj, "SOPClassUID").unwrap_or_else(|| "unknown".to_string());
        bail!(
            "{} is not a GSPS object (SOPClassUID={})",
            source,
            sop_class
        );
    }
    Ok(parse_gsps_overlays(&obj))
}

pub(crate) fn parse_gsps_overlays(obj: &DefaultDicomObject) -> HashMap<String, GspsOverlay> {
    const REFERENCED_SERIES_SEQUENCE: Tag = Tag(0x0008, 0x1115);
    const GRAPHIC_ANNOTATION_SEQUENCE: Tag = Tag(0x0070, 0x0001);

    let mut overlays_by_uid = HashMap::<String, GspsOverlay>::new();
    let default_refs = collect_root_referenced_image_targets(obj, REFERENCED_SERIES_SEQUENCE);

    let Some(annotations) = sequence_items_from_object(obj, GRAPHIC_ANNOTATION_SEQUENCE) else {
        return overlays_by_uid;
    };

    for annotation in annotations {
        let references = collect_item_referenced_image_targets(annotation);
        let target_refs = if references.is_empty() {
            &default_refs
        } else {
            &references
        };
        if target_refs.is_empty() {
            continue;
        }

        let graphics = collect_graphics_from_annotation(annotation);
        if graphics.is_empty() {
            continue;
        }

        for target in target_refs {
            overlays_by_uid
                .entry(target.sop_instance_uid.clone())
                .or_default()
                .graphics
                .extend(graphics.iter().cloned().map(|graphic| GspsOverlayGraphic {
                    graphic,
                    referenced_frames: target.referenced_frames.clone(),
                }));
        }
    }

    overlays_by_uid.retain(|_, overlay| !overlay.is_empty());
    overlays_by_uid
}

fn collect_root_referenced_image_targets(
    obj: &DefaultDicomObject,
    referenced_series_sequence: Tag,
) -> Vec<ReferencedImageTarget> {
    const REFERENCED_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x1140);

    let mut targets = Vec::new();
    if let Some(series_items) = sequence_items_from_object(obj, referenced_series_sequence) {
        for series_item in series_items {
            targets.extend(collect_item_referenced_image_targets(series_item));
        }
    }
    if targets.is_empty() {
        if let Some(image_items) = sequence_items_from_object(obj, REFERENCED_IMAGE_SEQUENCE) {
            for image_item in image_items {
                if let Some(target) = referenced_image_target_from_item(image_item) {
                    targets.push(target);
                }
            }
        }
    }
    targets.sort();
    targets.dedup();
    targets
}

fn collect_item_referenced_image_targets(item: &InMemDicomObject) -> Vec<ReferencedImageTarget> {
    const REFERENCED_IMAGE_SEQUENCE: Tag = Tag(0x0008, 0x1140);

    let mut targets = Vec::new();
    let Some(image_items) = sequence_items_from_item(item, REFERENCED_IMAGE_SEQUENCE) else {
        return targets;
    };
    for image_item in image_items {
        if let Some(target) = referenced_image_target_from_item(image_item) {
            targets.push(target);
        }
    }
    targets.sort();
    targets.dedup();
    targets
}

fn referenced_image_target_from_item(item: &InMemDicomObject) -> Option<ReferencedImageTarget> {
    const REFERENCED_SOP_INSTANCE_UID: Tag = Tag(0x0008, 0x1155);
    const REFERENCED_FRAME_NUMBER: Tag = Tag(0x0008, 0x1160);

    let sop_instance_uid = read_item_string(item, REFERENCED_SOP_INSTANCE_UID)?;
    let referenced_frames = read_item_multi_int(item, REFERENCED_FRAME_NUMBER).map(|frames| {
        let mut frames = frames
            .into_iter()
            .filter_map(|frame| usize::try_from(frame).ok())
            .filter(|frame| *frame > 0)
            .collect::<Vec<_>>();
        frames.sort_unstable();
        frames.dedup();
        if frames.is_empty() {
            log::warn!(
                "GSPS ReferencedFrameNumber was present but did not contain any usable frames."
            );
        }
        frames
    });

    Some(ReferencedImageTarget {
        sop_instance_uid,
        referenced_frames,
    })
}

fn collect_graphics_from_annotation(annotation: &InMemDicomObject) -> Vec<GspsGraphic> {
    const GRAPHIC_OBJECT_SEQUENCE: Tag = Tag(0x0070, 0x0009);

    let mut graphics = Vec::new();
    let Some(graphic_items) = sequence_items_from_item(annotation, GRAPHIC_OBJECT_SEQUENCE) else {
        return graphics;
    };

    for graphic_item in graphic_items {
        graphics.extend(collect_graphics_from_graphic_object(graphic_item));
    }
    graphics
}

fn collect_graphics_from_graphic_object(graphic_item: &InMemDicomObject) -> Vec<GspsGraphic> {
    const GRAPHIC_ANNOTATION_UNITS: Tag = Tag(0x0070, 0x0005);
    const GRAPHIC_DATA: Tag = Tag(0x0070, 0x0022);
    const GRAPHIC_TYPE: Tag = Tag(0x0070, 0x0023);
    const GRAPHIC_FILLED: Tag = Tag(0x0070, 0x0024);

    let units = match read_item_string(graphic_item, GRAPHIC_ANNOTATION_UNITS)
        .map(|value| value.to_ascii_uppercase())
    {
        Some(value) if value == "DISPLAY" => GspsUnits::Display,
        _ => GspsUnits::Pixel,
    };

    let points = read_item_multi_float(graphic_item, GRAPHIC_DATA)
        .map(parse_graphic_points)
        .unwrap_or_default();
    if points.is_empty() {
        return Vec::new();
    }

    let graphic_type = read_item_string(graphic_item, GRAPHIC_TYPE)
        .unwrap_or_else(|| "POLYLINE".to_string())
        .to_ascii_uppercase();

    match graphic_type.as_str() {
        "POINT" => points
            .into_iter()
            .map(|(x, y)| GspsGraphic::Point { x, y, units })
            .collect(),
        "CIRCLE" if points.len() >= 2 => {
            let polyline = approximate_circle(points[0], points[1]);
            vec![GspsGraphic::Polyline {
                points: polyline,
                units,
                closed: true,
            }]
        }
        "ELLIPSE" if points.len() >= 4 => {
            let polyline = approximate_ellipse(points[0], points[1], points[2], points[3]);
            vec![GspsGraphic::Polyline {
                points: polyline,
                units,
                closed: true,
            }]
        }
        _ => {
            let closed = read_item_string(graphic_item, GRAPHIC_FILLED)
                .is_some_and(|value| value.eq_ignore_ascii_case("Y"));
            vec![GspsGraphic::Polyline {
                points,
                units,
                closed,
            }]
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dicom::{EXPLICIT_VR_LITTLE_ENDIAN_UID, GSPS_SOP_CLASS_UID};
    use dicom_core::value::DataSetSequence;
    use dicom_core::{DataElement, PrimitiveValue, VR};
    use dicom_object::FileMetaTableBuilder;

    fn referenced_image_item(sop_instance_uid: &str) -> InMemDicomObject {
        InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1155),
            VR::UI,
            sop_instance_uid,
        )])
    }

    fn referenced_image_item_with_frames(
        sop_instance_uid: &str,
        referenced_frames: &[i32],
    ) -> InMemDicomObject {
        let mut item = referenced_image_item(sop_instance_uid);
        item.put(DataElement::new(
            Tag(0x0008, 0x1160),
            VR::IS,
            referenced_frames
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join("\\"),
        ));
        item
    }

    fn graphic_object_item(
        graphic_type: &str,
        graphic_data: &[f32],
        units: &str,
        filled: Option<&str>,
    ) -> InMemDicomObject {
        let mut item = InMemDicomObject::new_empty();
        item.put(DataElement::new(Tag(0x0070, 0x0005), VR::CS, units));
        item.put(DataElement::new(
            Tag(0x0070, 0x0022),
            VR::FL,
            PrimitiveValue::F32(graphic_data.iter().copied().collect()),
        ));
        item.put(DataElement::new(Tag(0x0070, 0x0023), VR::CS, graphic_type));
        if let Some(flag) = filled {
            item.put(DataElement::new(Tag(0x0070, 0x0024), VR::CS, flag));
        }
        item
    }

    #[test]
    fn parse_graphic_points_reads_xy_pairs() {
        let points = parse_graphic_points(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0]);
        assert_eq!(points, vec![(10.0, 20.0), (30.0, 40.0), (50.0, 60.0)]);
    }

    #[test]
    fn collect_referenced_image_targets_deduplicates_exact_matches() {
        let item = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1140),
            VR::SQ,
            DataSetSequence::from(vec![
                referenced_image_item("1.2.3"),
                referenced_image_item("1.2.3"),
                referenced_image_item_with_frames("1.2.3", &[1, 3]),
                referenced_image_item_with_frames("1.2.3", &[1, 3]),
                referenced_image_item("2.4.6"),
            ]),
        )]);

        let refs = collect_item_referenced_image_targets(&item);
        assert_eq!(
            refs,
            vec![
                ReferencedImageTarget {
                    sop_instance_uid: "1.2.3".to_string(),
                    referenced_frames: None,
                },
                ReferencedImageTarget {
                    sop_instance_uid: "1.2.3".to_string(),
                    referenced_frames: Some(vec![1, 3]),
                },
                ReferencedImageTarget {
                    sop_instance_uid: "2.4.6".to_string(),
                    referenced_frames: None,
                },
            ]
        );
    }

    #[test]
    fn collect_graphic_object_parses_polyline_units_and_closed_flag() {
        let item = graphic_object_item(
            "POLYLINE",
            &[0.0, 0.0, 1.0, 1.0, 2.0, 2.0],
            "DISPLAY",
            Some("Y"),
        );
        let graphics = collect_graphics_from_graphic_object(&item);
        assert_eq!(graphics.len(), 1);

        match &graphics[0] {
            GspsGraphic::Polyline {
                points,
                units,
                closed,
            } => {
                assert_eq!(points.len(), 3);
                assert_eq!(*units, GspsUnits::Display);
                assert!(*closed);
            }
            other => panic!("Expected polyline, got {other:?}"),
        }
    }

    #[test]
    fn parse_gsps_overlays_maps_annotation_reference_to_target_sop() {
        let annotation = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1140),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image_item("1.2.840.1")]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0009),
                VR::SQ,
                DataSetSequence::from(vec![graphic_object_item(
                    "POINT",
                    &[100.0, 120.0],
                    "PIXEL",
                    None,
                )]),
            ),
        ]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.9.9.9"),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("9.9.9.9"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        let overlay = overlays
            .get("1.2.840.1")
            .expect("Overlay should be mapped to referenced SOP instance");
        assert_eq!(overlay.graphics.len(), 1);
        assert!(overlay.graphics[0].referenced_frames.is_none());
    }

    #[test]
    fn parse_gsps_overlays_tracks_annotation_referenced_frames() {
        let annotation = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1140),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image_item_with_frames(
                    "1.2.840.1",
                    &[2, 4],
                )]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0009),
                VR::SQ,
                DataSetSequence::from(vec![graphic_object_item(
                    "POINT",
                    &[100.0, 120.0],
                    "PIXEL",
                    None,
                )]),
            ),
        ]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.9.9.10"),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("9.9.9.10"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        let overlay = overlays
            .get("1.2.840.1")
            .expect("Overlay should be mapped to referenced SOP instance");
        assert_eq!(overlay.graphics.len(), 1);
        assert_eq!(overlay.graphics[0].referenced_frames, Some(vec![2, 4]));
        assert_eq!(overlay.graphics_for_frame(0).count(), 0);
        assert_eq!(overlay.graphics_for_frame(1).count(), 1);
        assert_eq!(overlay.graphics_for_frame(3).count(), 1);
    }

    #[test]
    fn parse_gsps_overlays_preserves_invalid_referenced_frames_as_empty() {
        let annotation = InMemDicomObject::from_element_iter([
            DataElement::new(
                Tag(0x0008, 0x1140),
                VR::SQ,
                DataSetSequence::from(vec![referenced_image_item_with_frames(
                    "1.2.840.1",
                    &[0, -1],
                )]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0009),
                VR::SQ,
                DataSetSequence::from(vec![graphic_object_item(
                    "POINT",
                    &[100.0, 120.0],
                    "PIXEL",
                    None,
                )]),
            ),
        ]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "9.9.9.11"),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("9.9.9.11"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        let overlay = overlays
            .get("1.2.840.1")
            .expect("Overlay should be mapped to referenced SOP instance");
        assert_eq!(overlay.graphics.len(), 1);
        assert_eq!(overlay.graphics[0].referenced_frames, Some(Vec::new()));
        assert_eq!(overlay.graphics_for_frame(0).count(), 0);
        assert_eq!(overlay.graphics_for_frame(1).count(), 0);
    }

    #[test]
    fn parse_gsps_overlays_uses_root_reference_when_annotation_reference_missing() {
        let series_item = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0008, 0x1140),
            VR::SQ,
            DataSetSequence::from(vec![referenced_image_item_with_frames("7.7.7.7", &[3])]),
        )]);
        let annotation = InMemDicomObject::from_element_iter([DataElement::new(
            Tag(0x0070, 0x0009),
            VR::SQ,
            DataSetSequence::from(vec![graphic_object_item(
                "POLYLINE",
                &[10.0, 10.0, 20.0, 20.0],
                "PIXEL",
                None,
            )]),
        )]);

        let gsps_dataset = InMemDicomObject::from_element_iter([
            DataElement::new(Tag(0x0008, 0x0016), VR::UI, GSPS_SOP_CLASS_UID),
            DataElement::new(Tag(0x0008, 0x0018), VR::UI, "8.8.8.8"),
            DataElement::new(
                Tag(0x0008, 0x1115),
                VR::SQ,
                DataSetSequence::from(vec![series_item]),
            ),
            DataElement::new(
                Tag(0x0070, 0x0001),
                VR::SQ,
                DataSetSequence::from(vec![annotation]),
            ),
        ]);

        let gsps_obj = gsps_dataset
            .with_meta(
                FileMetaTableBuilder::new()
                    .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                    .media_storage_sop_class_uid(GSPS_SOP_CLASS_UID)
                    .media_storage_sop_instance_uid("8.8.8.8"),
            )
            .expect("GSPS test object should build file meta");

        let overlays = parse_gsps_overlays(&gsps_obj);
        assert!(
            overlays.contains_key("7.7.7.7"),
            "Root-level references should be used when annotation-level references are absent"
        );
        let overlay = overlays
            .get("7.7.7.7")
            .expect("Overlay should exist for root-level referenced SOP instance");
        assert_eq!(overlay.graphics[0].referenced_frames, Some(vec![3]));
    }
}
