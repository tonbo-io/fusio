#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub(crate) fn wasm_err(js_val: js_sys::wasm_bindgen::JsValue) -> fusio_core::Error {
    fusio_core::Error::Wasm {
        message: format!("{js_val:?}"),
    }
}
