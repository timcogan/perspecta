use std::{fmt, path::Path};

use dicom_core::Tag;
use dicom_object::DefaultDicomObject;
use serde::Serialize;

use super::{classify_dicom_object, open_dicom_object, DicomPathKind};

pub(crate) const MIB_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Serialize)]
pub(crate) struct Inspection {
    schema_version: u8,
    kind: InspectionKind,
    modality: Option<String>,
    sop_class_uid: Option<String>,
    transfer_syntax_uid: Option<String>,
    rows: Option<u32>,
    columns: Option<u32>,
    number_of_frames: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum InspectionKind {
    Image,
    StructuredReport,
    Gsps,
    ParametricMap,
    Other,
}

#[derive(Debug)]
pub(crate) enum InspectError {
    Read,
    FileTooLarge { size_bytes: u64, limit_bytes: u64 },
    InvalidMetadata(&'static str),
}

impl fmt::Display for InspectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read => f.write_str(
                "Could not read the DICOM file. Check the file, its permissions, and its encoding.",
            ),
            Self::FileTooLarge { size_bytes, limit_bytes } => write!(
                f,
                "DICOM file size is {size_bytes} bytes ({} MiB rounded up). \
                 This exceeds the configured limit of {} MiB ({limit_bytes} bytes). \
                 Retry with --max-file-mib {} or higher. \
                 The complete file is read into memory. Parsing and repairs can require additional memory.",
                size_bytes.div_ceil(MIB_BYTES),
                limit_bytes / MIB_BYTES,
                size_bytes.div_ceil(MIB_BYTES),
            ),
            Self::InvalidMetadata(field) => {
                write!(f, "Invalid {field} value in the DICOM dataset.")
            }
        }
    }
}

impl std::error::Error for InspectError {}

pub(crate) fn inspect_file(path: &Path, limit_bytes: u64) -> Result<Inspection, InspectError> {
    let object = open_file_with_limit(path, limit_bytes)?;
    inspect_object(&object)
}

pub(super) fn open_file_with_limit(
    path: &Path,
    limit_bytes: u64,
) -> Result<DefaultDicomObject, InspectError> {
    // Only regular files are inputs. In particular, do not block on a pipe or device.
    let metadata = path.metadata().map_err(|_| InspectError::Read)?;
    if !metadata.is_file() {
        return Err(InspectError::Read);
    }
    if metadata.len() > limit_bytes {
        return Err(InspectError::FileTooLarge {
            size_bytes: metadata.len(),
            limit_bytes,
        });
    }
    // Reuse the viewer's reader and repairs. Do not expose
    // reader errors: they can contain a file path or values outside this summary.
    open_dicom_object(path).map_err(|_| InspectError::Read)
}

fn inspect_object(object: &DefaultDicomObject) -> Result<Inspection, InspectError> {
    let kind = match classify_dicom_object(object) {
        DicomPathKind::Image => InspectionKind::Image,
        DicomPathKind::StructuredReport => InspectionKind::StructuredReport,
        DicomPathKind::Gsps => InspectionKind::Gsps,
        DicomPathKind::ParametricMap => InspectionKind::ParametricMap,
        DicomPathKind::Other => InspectionKind::Other,
    };
    let has_dimensions = matches!(kind, InspectionKind::Image | InspectionKind::ParametricMap);
    Ok(Inspection {
        schema_version: 1,
        kind,
        modality: read_text(object, Tag(0x0008, 0x0060), "Modality")?,
        sop_class_uid: read_text(object, Tag(0x0008, 0x0016), "SOPClassUID")?,
        transfer_syntax_uid: nonempty(object.meta().transfer_syntax()),
        rows: if has_dimensions {
            read_positive_integer(object, Tag(0x0028, 0x0010), "Rows", u16::MAX.into())?
        } else {
            None
        },
        columns: if has_dimensions {
            read_positive_integer(object, Tag(0x0028, 0x0011), "Columns", u16::MAX.into())?
        } else {
            None
        },
        number_of_frames: if has_dimensions {
            read_positive_integer(object, Tag(0x0028, 0x0008), "NumberOfFrames", u32::MAX)?
        } else {
            None
        },
    })
}

fn nonempty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn read_text(
    object: &DefaultDicomObject,
    tag: Tag,
    field: &'static str,
) -> Result<Option<String>, InspectError> {
    let Some(element) = object
        .element_opt(tag)
        .map_err(|_| InspectError::InvalidMetadata(field))?
    else {
        return Ok(None);
    };
    let value = element
        .to_str()
        .map_err(|_| InspectError::InvalidMetadata(field))?;
    Ok(nonempty(&value))
}

pub(super) fn read_positive_integer(
    object: &DefaultDicomObject,
    tag: Tag,
    field: &'static str,
    maximum: u32,
) -> Result<Option<u32>, InspectError> {
    let Some(element) = object
        .element_opt(tag)
        .map_err(|_| InspectError::InvalidMetadata(field))?
    else {
        return Ok(None);
    };
    let value = element
        .to_str()
        .map_err(|_| InspectError::InvalidMetadata(field))?;
    let value = value
        .trim()
        .parse::<u32>()
        .map_err(|_| InspectError::InvalidMetadata(field))?;
    if value == 0 || value > maximum {
        return Err(InspectError::InvalidMetadata(field));
    }
    Ok(Some(value))
}
