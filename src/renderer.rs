use eframe::egui::{Color32, ColorImage};

pub fn render_window_level(
    width_px: usize,
    height_px: usize,
    frame_pixels: &[i32],
    invert: bool,
    center: f32,
    width: f32,
) -> ColorImage {
    let effective_width = width.max(1.0);
    let low = center - effective_width / 2.0;
    let high = center + effective_width / 2.0;
    let range = (high - low).max(1e-6);

    let mut pixels = Vec::with_capacity(frame_pixels.len());
    for &sample in frame_pixels {
        let normalized = ((sample as f32 - low) / range).clamp(0.0, 1.0);
        let mut gray = (normalized * 255.0).round() as u8;
        if invert {
            gray = 255 - gray;
        }
        pixels.push(Color32::from_gray(gray));
    }

    ColorImage {
        size: [width_px, height_px],
        pixels,
    }
}

pub fn render_rgb(
    width_px: usize,
    height_px: usize,
    frame_pixels: &[u8],
    samples_per_pixel: u16,
) -> ColorImage {
    let spp = samples_per_pixel.max(1) as usize;
    let pixel_count = width_px.saturating_mul(height_px);
    let mut pixels = Vec::with_capacity(pixel_count);

    for chunk in frame_pixels.chunks_exact(spp).take(pixel_count) {
        let r = chunk[0];
        let g = if spp > 1 { chunk[1] } else { r };
        let b = if spp > 2 { chunk[2] } else { r };
        pixels.push(Color32::from_rgb(r, g, b));
    }

    if pixels.len() < pixel_count {
        pixels.resize(pixel_count, Color32::BLACK);
    }

    ColorImage {
        size: [width_px, height_px],
        pixels,
    }
}
