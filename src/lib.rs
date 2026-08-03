mod app;
mod dicom;
#[cfg(not(target_arch = "wasm32"))]
mod dicomweb;
#[cfg(target_arch = "wasm32")]
#[path = "dicomweb_web.rs"]
mod dicomweb;
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
mod launch;
#[cfg(not(target_arch = "wasm32"))]
mod logging;
mod mammo;
mod platform;
mod renderer;

#[cfg(not(target_arch = "wasm32"))]
use std::io;

#[cfg(not(target_arch = "wasm32"))]
pub fn run_native() -> eframe::Result<()> {
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

#[cfg(target_arch = "wasm32")]
mod web {
    use std::cell::RefCell;

    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    use crate::app::{self, DicomViewerApp};

    thread_local! {
        static WEB_RUNNER: RefCell<Option<eframe::WebRunner>> = const { RefCell::new(None) };
    }

    /// Read one browser drop as an atomic file selection for the viewer.
    #[wasm_bindgen]
    pub async fn queue_dropped_files(files: js_sys::Array) -> Result<(), JsValue> {
        let _drop_guard =
            app::begin_web_drop_read().map_err(|message| JsValue::from_str(&message))?;
        if files.length() == 0 {
            return Err(JsValue::from_str("No local files were dropped."));
        }

        let mut selected_files = Vec::with_capacity(files.length() as usize);
        let mut selected_sizes = Vec::with_capacity(files.length() as usize);
        for value in files.iter() {
            let file = value
                .dyn_into::<web_sys::File>()
                .map_err(|_| JsValue::from_str("A dropped item was not a local file."))?;
            let byte_len = app::checked_web_file_size(file.size()).map_err(JsValue::from_str)?;
            selected_files.push((file, byte_len));
            selected_sizes.push(byte_len);
        }
        app::checked_web_session_byte_total(0, selected_sizes).map_err(JsValue::from_str)?;

        let mut selected = Vec::with_capacity(selected_files.len());
        for (file, expected_len) in selected_files {
            let buffer = JsFuture::from(file.array_buffer())
                .await
                .map_err(|_| JsValue::from_str("The browser could not read a dropped file."))?;
            let bytes = js_sys::Uint8Array::new(&buffer).to_vec();
            if bytes.len() != expected_len {
                return Err(JsValue::from_str(
                    "A dropped file changed size while the browser was reading it.",
                ));
            }
            selected.push((file.name(), bytes));
            crate::platform::yield_to_browser().await;
        }

        app::queue_web_dropped_files(selected).map_err(|message| JsValue::from_str(&message))
    }

    /// Start Perspecta in an existing canvas. DICOM bytes remain in browser memory.
    #[wasm_bindgen]
    pub async fn start_perspecta(canvas_id: String) -> Result<(), JsValue> {
        // The preview deliberately avoids emitting selected-file details to the console.
        let _ = eframe::WebLogger::init(log::LevelFilter::Off);

        let window = web_sys::window().ok_or_else(|| JsValue::from_str("Window unavailable"))?;
        let document = window
            .document()
            .ok_or_else(|| JsValue::from_str("Document unavailable"))?;
        let canvas = document
            .get_element_by_id(&canvas_id)
            .ok_or_else(|| JsValue::from_str("Perspecta canvas not found"))?
            .dyn_into::<web_sys::HtmlCanvasElement>()
            .map_err(|_| JsValue::from_str("Perspecta target is not a canvas"))?;

        let runner = eframe::WebRunner::new();
        WEB_RUNNER.with(|slot| slot.replace(Some(runner.clone())));

        runner
            .start(
                canvas,
                eframe::WebOptions::default(),
                Box::new(|cc| {
                    app::set_web_drop_repaint_context(cc.egui_ctx.clone());
                    Ok(Box::new(DicomViewerApp::new(None)))
                }),
            )
            .await
    }
}

#[cfg(target_arch = "wasm32")]
pub use web::{queue_dropped_files, start_perspecta};
