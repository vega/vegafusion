use prost::Message;
use vegafusion_core::proto::gen::services::QueryRequest;
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use wasm_bindgen::prelude::*;
use js_sys::Promise;
use std::sync::Arc;
use vegafusion_runtime::datafusion::context::make_datafusion_context;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;
use web_sys::console;


fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
pub struct VegaFusionRuntimeHandle {
    runtime: Arc<VegaFusionRuntime>,
}

#[wasm_bindgen]
impl VegaFusionRuntimeHandle {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        set_panic_hook();
        let ctx = make_datafusion_context();
        let runtime = Arc::new(VegaFusionRuntime::new(Arc::new(ctx), None, None));
        Self { runtime }
    }

    pub fn query(&self, data: JsValue) -> Result<Promise, JsValue> {
        let runtime = self.runtime.clone();
        Ok(wasm_bindgen_futures::future_to_promise(async move {
            let data_array = data.dyn_into::<js_sys::Uint8Array>()?;
            let query_request_bytes = data_array.to_vec();
            let query_request = QueryRequest::decode(query_request_bytes.as_slice()).unwrap();

            let query_result_future = runtime.query_request_message(query_request);
            let query_result = query_result_future.await.unwrap();
            let query_result_bytes = query_result.encode_to_vec();
            let query_result_uint8array = js_sys::Uint8Array::from(query_result_bytes.as_slice());
            Ok(JsValue::from(query_result_uint8array))
        }))
    }
}
