#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<std::process::ExitCode> {
    perspecta::run()
}

#[cfg(target_arch = "wasm32")]
fn main() {}
