use anyhow::{bail, Result};
use dicom_object::{DefaultDicomObject, InMemDicomObject, Tag};

use super::{
    classify_dicom_object, collect_metadata, open_dicom_object, read_item_string, read_string,
    sequence_items_from_item, sequence_items_from_object, DicomPathKind, DicomSource,
};

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
    }
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
