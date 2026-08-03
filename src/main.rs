#[cfg(not(target_arch = "wasm32"))]
fn main() -> eframe::Result<()> {
    perspecta::run_native()
}

#[cfg(target_arch = "wasm32")]
fn main() {}
