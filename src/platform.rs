#[cfg(not(target_arch = "wasm32"))]
pub(crate) type MonotonicInstant = std::time::Instant;

#[cfg(target_arch = "wasm32")]
pub(crate) type MonotonicInstant = web_time::Instant;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(task: F)
where
    F: FnOnce() + Send + 'static,
{
    let _ = std::thread::spawn(task);
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(task: F)
where
    F: FnOnce() + 'static,
{
    use wasm_bindgen::closure::Closure;
    let callback = Closure::once_into_js(task);
    schedule_once_or_invoke(&callback);
}

#[cfg(target_arch = "wasm32")]
thread_local! {
    static WEB_MAX_TEXTURE_SIDE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn set_web_max_texture_side(max_texture_side: usize) {
    WEB_MAX_TEXTURE_SIDE.with(|value| value.set(max_texture_side));
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn web_max_texture_side() -> Option<usize> {
    let value = WEB_MAX_TEXTURE_SIDE.with(std::cell::Cell::get);
    (value > 0).then_some(value)
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn yield_to_browser() {
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::JsValue;

    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let resolve = resolve.clone();
        let callback = Closure::once_into_js(move || {
            let _ = resolve.call0(&JsValue::UNDEFINED);
        });
        schedule_once_or_invoke(&callback);
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(target_arch = "wasm32")]
fn schedule_once_or_invoke(callback: &wasm_bindgen::JsValue) {
    use wasm_bindgen::JsCast as _;

    let queued = web_sys::window().is_some_and(|window| {
        window
            .set_timeout_with_callback_and_timeout_and_arguments_0(callback.unchecked_ref(), 0)
            .is_ok()
    });
    if queued {
        return;
    }

    if let Some(callback) = callback.dyn_ref::<js_sys::Function>() {
        let _ = callback.call0(&wasm_bindgen::JsValue::UNDEFINED);
    }
}
