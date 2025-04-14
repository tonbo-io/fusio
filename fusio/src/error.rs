pub use fusio_core::error::{BoxedError, Error};

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub(crate) fn wasm_err(js_val: js_sys::wasm_bindgen::JsValue) -> Error {
    Error::Wasm {
        message: format!("{js_val:?}"),
    }
}
