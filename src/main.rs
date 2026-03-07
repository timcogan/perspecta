mod app;
mod dicom;
mod dicomweb;
mod launch;
mod logging;
mod mammo;
mod renderer;

use std::io;

fn main() -> eframe::Result<()> {
    logging::init().map_err(|err| eframe::Error::AppCreation(Box::new(err)))?;

    let cli_args = std::env::args().skip(1).collect::<Vec<_>>();
    let initial_request = launch::parse_launch_request_from_args(&cli_args).map_err(|err| {
        eframe::Error::AppCreation(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Launch URL/args error: {err}"),
        )))
    })?;

    let native_options = eframe::NativeOptions {
        viewport: eframe::egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 820.0])
            .with_decorations(false)
            .with_resizable(true),
        ..Default::default()
    };

    eframe::run_native(
        "Perspecta Viewer",
        native_options,
        Box::new(move |_cc| Ok(Box::new(app::DicomViewerApp::new(initial_request.clone())))),
    )
}
