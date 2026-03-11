use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dicom_core::value::PrimitiveValue;
use dicom_core::{dicom_value, DataElement, Tag, VR};
use dicom_object::{FileMetaTableBuilder, InMemDicomObject};

const SECONDARY_CAPTURE_IMAGE_STORAGE_UID: &str = "1.2.840.10008.5.1.4.1.1.7";
const EXPLICIT_VR_LITTLE_ENDIAN_UID: &str = "1.2.840.10008.1.2.1";

pub struct TempBenchmarkDir {
    path: PathBuf,
}

impl TempBenchmarkDir {
    pub fn new(prefix: &str) -> Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = env::temp_dir().join(format!(
            "perspecta-{prefix}-{}-{timestamp}",
            std::process::id()
        ));
        fs::create_dir_all(&path)
            .with_context(|| format!("could not create temporary directory {}", path.display()))?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBenchmarkDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn write_synthetic_dicom(path: &Path, rows: usize, cols: usize) -> Result<()> {
    let sop_instance_uid = format!(
        "2.25.{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let rows_u16 = u16::try_from(rows).context("rows exceed u16 range")?;
    let cols_u16 = u16::try_from(cols).context("cols exceed u16 range")?;
    let pixels = generate_pixels(rows, cols);

    let obj = InMemDicomObject::from_element_iter([
        DataElement::new(
            Tag(0x0008, 0x0016),
            VR::UI,
            dicom_value!(Strs, [SECONDARY_CAPTURE_IMAGE_STORAGE_UID]),
        ),
        DataElement::new(
            Tag(0x0008, 0x0018),
            VR::UI,
            dicom_value!(Strs, [sop_instance_uid.as_str()]),
        ),
        DataElement::new(Tag(0x0008, 0x0060), VR::CS, dicom_value!(Strs, ["OT"])),
        DataElement::new(Tag(0x0028, 0x0002), VR::US, dicom_value!(U16, [1])),
        DataElement::new(
            Tag(0x0028, 0x0004),
            VR::CS,
            dicom_value!(Strs, ["MONOCHROME2"]),
        ),
        DataElement::new(Tag(0x0028, 0x0010), VR::US, dicom_value!(U16, [rows_u16])),
        DataElement::new(Tag(0x0028, 0x0011), VR::US, dicom_value!(U16, [cols_u16])),
        DataElement::new(Tag(0x0028, 0x0100), VR::US, dicom_value!(U16, [16])),
        DataElement::new(Tag(0x0028, 0x0101), VR::US, dicom_value!(U16, [16])),
        DataElement::new(Tag(0x0028, 0x0102), VR::US, dicom_value!(U16, [15])),
        DataElement::new(Tag(0x0028, 0x0103), VR::US, dicom_value!(U16, [0])),
        DataElement::new(
            Tag(0x7FE0, 0x0010),
            VR::OW,
            PrimitiveValue::U16(pixels.into()),
        ),
    ]);

    let file_obj = obj
        .with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax(EXPLICIT_VR_LITTLE_ENDIAN_UID)
                .media_storage_sop_class_uid(SECONDARY_CAPTURE_IMAGE_STORAGE_UID)
                .media_storage_sop_instance_uid(sop_instance_uid),
        )
        .context("could not build DICOM file meta table")?;
    file_obj
        .write_to_file(path)
        .with_context(|| format!("could not write synthetic DICOM {}", path.display()))?;
    Ok(())
}

fn generate_pixels(rows: usize, cols: usize) -> Vec<u16> {
    let pixel_count = rows.saturating_mul(cols);
    let mut pixels = Vec::with_capacity(pixel_count);
    for index in 0..pixel_count {
        let row = index / cols.max(1);
        let col = index % cols.max(1);
        let value = (((row * 17) + (col * 31)) % u16::MAX as usize) as u16;
        pixels.push(value);
    }
    pixels
}
