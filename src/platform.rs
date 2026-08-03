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
/// Queue browser work outside the caller's stack so load setup cannot re-enter
/// application code before its receiver and in-flight state are installed.
pub(crate) fn spawn<F>(task: F)
where
    F: FnOnce() + 'static,
{
    use wasm_bindgen::closure::Closure;
    let callback = Closure::once_into_js(task);
    if schedule_once(&callback) || schedule_once_via_message_channel(&callback) {
        return;
    }
    schedule_once_via_microtask(&callback);
}

#[cfg(target_arch = "wasm32")]
thread_local! {
    static WEB_MAX_TEXTURE_SIDE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static MESSAGE_TASK_QUEUE: std::cell::RefCell<std::collections::VecDeque<wasm_bindgen::JsValue>> =
        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
    static MESSAGE_TASK_SCHEDULER: Option<MessageTaskScheduler> = MessageTaskScheduler::new();
}

#[cfg(target_arch = "wasm32")]
struct MessageTaskScheduler {
    sender: web_sys::MessagePort,
    _receiver: web_sys::MessagePort,
    _onmessage: wasm_bindgen::closure::Closure<dyn FnMut()>,
}

#[cfg(target_arch = "wasm32")]
impl MessageTaskScheduler {
    fn new() -> Option<Self> {
        use wasm_bindgen::JsCast as _;

        let channel = web_sys::MessageChannel::new().ok()?;
        let receiver = channel.port1();
        let sender = channel.port2();
        let onmessage = wasm_bindgen::closure::Closure::new(move || {
            let callback = MESSAGE_TASK_QUEUE.with(|queue| queue.borrow_mut().pop_front());
            if let Some(callback) = callback
                .as_ref()
                .and_then(|callback| callback.dyn_ref::<js_sys::Function>())
            {
                let _ = callback.call0(&wasm_bindgen::JsValue::UNDEFINED);
            }
        });
        receiver.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        receiver.start();
        Some(Self {
            sender,
            _receiver: receiver,
            _onmessage: onmessage,
        })
    }
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
        if !schedule_once_via_message_channel(&callback) {
            schedule_once_or_invoke(&callback);
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

#[cfg(target_arch = "wasm32")]
fn schedule_once(callback: &wasm_bindgen::JsValue) -> bool {
    use wasm_bindgen::JsCast as _;

    web_sys::window().is_some_and(|window| {
        window
            .set_timeout_with_callback_and_timeout_and_arguments_0(callback.unchecked_ref(), 0)
            .is_ok()
    })
}

#[cfg(target_arch = "wasm32")]
fn schedule_once_via_message_channel(callback: &wasm_bindgen::JsValue) -> bool {
    MESSAGE_TASK_QUEUE.with(|queue| queue.borrow_mut().push_back(callback.clone()));
    let queued = MESSAGE_TASK_SCHEDULER.with(|scheduler| {
        scheduler.as_ref().is_some_and(|scheduler| {
            scheduler
                .sender
                .post_message(&wasm_bindgen::JsValue::UNDEFINED)
                .is_ok()
        })
    });
    if queued {
        return true;
    }

    MESSAGE_TASK_QUEUE.with(|queue| {
        let _ = queue.borrow_mut().pop_back();
    });
    false
}

#[cfg(target_arch = "wasm32")]
fn schedule_once_via_microtask(callback: &wasm_bindgen::JsValue) {
    use wasm_bindgen::JsCast as _;

    if let Some(callback) = callback.dyn_ref::<js_sys::Function>() {
        if let Some(window) = web_sys::window() {
            window.queue_microtask(callback);
            return;
        }

        let promise = js_sys::Promise::resolve(&wasm_bindgen::JsValue::UNDEFINED);
        let then = js_sys::Reflect::get(&promise, &wasm_bindgen::JsValue::from_str("then"))
            .ok()
            .and_then(|then| then.dyn_into::<js_sys::Function>().ok());
        if let Some(then) = then {
            let _ = then.call1(&promise, callback);
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn schedule_once_or_invoke(callback: &wasm_bindgen::JsValue) {
    use wasm_bindgen::JsCast as _;

    if schedule_once(callback) {
        return;
    }

    if let Some(callback) = callback.dyn_ref::<js_sys::Function>() {
        let _ = callback.call0(&wasm_bindgen::JsValue::UNDEFINED);
    }
}
