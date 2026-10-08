use std::{fmt, path::Path};

use dicom_core::Tag;
use dicom_object::DefaultDicomObject;
use dicom_pixeldata::{PhotometricInterpretation, PixelDecoder};

use super::inspect::{open_file_with_limit, read_positive_integer, InspectError};
use super::{classify_dicom_object, min_max, read_float_first, DicomPathKind};
use crate::renderer::{render_rgb, render_window_level};

#[derive(Debug)]
pub(crate) enum RenderError {
    Inspect(InspectError),
    Unsupported,
    FrameOutOfRange { frame: u32, frame_count: u32 },
    Decode,
    Output,
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inspect(error) => error.fmt(f),
            Self::Unsupported => f.write_str(
                "PNG export requires an 8-bit or 16-bit monochrome or RGB image. Reports, presentation states, and Parametric Maps are not supported.",
            ),
            Self::FrameOutOfRange { frame, frame_count } => write!(
                f,
                "Requested frame {frame}, but the DICOM contains {frame_count} frames. Use --frame from 1 to {frame_count} in stored DICOM order.",
            ),
            Self::Decode => f.write_str(
                "Could not decode the requested DICOM frame. Check the pixel data and whether this build supports its transfer syntax and pixel layout.",
            ),
            Self::Output => f.write_str("Could not encode the PNG output."),
        }
    }
}

impl std::error::Error for RenderError {}

impl From<InspectError> for RenderError {
    fn from(error: InspectError) -> Self {
        Self::Inspect(error)
    }
}

/// Render one stored frame, without viewer state, metadata overlays, or preload workers.
pub(crate) fn render_file(
    path: &Path,
    limit_bytes: u64,
    frame: u32,
) -> Result<Vec<u8>, RenderError> {
    let object = open_file_with_limit(path, limit_bytes)?;
    if !matches!(classify_dicom_object(&object), DicomPathKind::Image) {
        return Err(RenderError::Unsupported);
    }
    let frame_count =
        read_positive_integer(&object, Tag(0x0028, 0x0008), "NumberOfFrames", u32::MAX)?
            .unwrap_or(1);
    if frame == 0 || frame > frame_count {
        return Err(RenderError::FrameOutOfRange { frame, frame_count });
    }
    let rows = required_u16(&object, Tag(0x0028, 0x0010), "Rows")?;
    let columns = required_u16(&object, Tag(0x0028, 0x0011), "Columns")?;
    let samples = required_u16(&object, Tag(0x0028, 0x0002), "SamplesPerPixel")?;
    let bits = required_u16(&object, Tag(0x0028, 0x0100), "BitsAllocated")?;
    let stored = required_u16(&object, Tag(0x0028, 0x0101), "BitsStored")?;
    if !matches!(samples, 1 | 3) || !matches!(bits, 8 | 16) {
        return Err(RenderError::Unsupported);
    }
    if stored > bits {
        return Err(InspectError::InvalidMetadata("BitsStored").into());
    }
    let width = columns as usize;
    let height = rows as usize;
    let sample_count = width
        .checked_mul(height)
        .and_then(|count| count.checked_mul(samples as usize))
        .ok_or(RenderError::Decode)?;
    // Check the frame offset before the decoder uses native-sized arithmetic.
    sample_count
        .checked_mul(usize::from(bits / 8))
        .and_then(|size| size.checked_mul(frame as usize))
        .ok_or(RenderError::Decode)?;
    let decoded = object
        .decode_pixel_data_frame(frame - 1)
        .map_err(|_| RenderError::Decode)?;
    if decoded.columns() != u32::from(columns)
        || decoded.rows() != u32::from(rows)
        || decoded.samples_per_pixel() != samples
        || decoded.bits_allocated() != bits
    {
        return Err(RenderError::Decode);
    }
    let image = match (samples, decoded.photometric_interpretation()) {
        (1, PhotometricInterpretation::Monochrome1 | PhotometricInterpretation::Monochrome2) => {
            let pixels: Vec<i32> = decoded.to_vec_frame(0).map_err(|_| RenderError::Decode)?;
            if pixels.len() != sample_count {
                return Err(RenderError::Decode);
            }
            let (min, max) = min_max(&pixels).ok_or(RenderError::Decode)?;
            let center = read_float_first(&object, "WindowCenter")
                .filter(|value| value.is_finite())
                .unwrap_or((min as f32 + max as f32) / 2.0);
            let window = read_float_first(&object, "WindowWidth")
                .filter(|value| value.is_finite())
                .unwrap_or((max as f32 - min as f32).max(1.0));
            render_window_level(
                width,
                height,
                &pixels,
                *decoded.photometric_interpretation() == PhotometricInterpretation::Monochrome1,
                center,
                window,
            )
        }
        (3, PhotometricInterpretation::Rgb) => {
            let pixels: Vec<u8> = if bits == 8 {
                decoded.to_vec_frame(0).map_err(|_| RenderError::Decode)?
            } else {
                let pixels: Vec<u16> = decoded.to_vec_frame(0).map_err(|_| RenderError::Decode)?;
                pixels
                    .into_iter()
                    .map(|value| (value >> stored.saturating_sub(8)) as u8)
                    .collect()
            };
            if pixels.len() != sample_count {
                return Err(RenderError::Decode);
            }
            render_rgb(width, height, &pixels, samples)
        }
        _ => return Err(RenderError::Unsupported),
    };
    let pixels: Vec<u8> = image
        .pixels
        .iter()
        .flat_map(|pixel| pixel.to_array())
        .collect();
    let mut png = Vec::new();
    // Only dimensions and rendered pixels enter the PNG, never DICOM metadata.
    let mut encoder = png::Encoder::new(&mut png, columns.into(), rows.into());
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().map_err(|_| RenderError::Output)?;
    writer
        .write_image_data(&pixels)
        .map_err(|_| RenderError::Output)?;
    writer.finish().map_err(|_| RenderError::Output)?;
    Ok(png)
}

fn required_u16(
    object: &DefaultDicomObject,
    tag: Tag,
    field: &'static str,
) -> Result<u16, InspectError> {
    read_positive_integer(object, tag, field, u16::MAX.into())?
        .map(|value| value as u16)
        .ok_or(InspectError::InvalidMetadata(field))
}
