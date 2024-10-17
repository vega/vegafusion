use futures::{SinkExt, StreamExt};
use prost::Message;

use vegafusion_common::data::scalar::{ScalarValue, ScalarValueHelpers};
use vegafusion_core::proto::gen::tasks::{NodeValueIndex, ResponseTaskValue, TaskGraph, TaskGraphValueRequest, TzConfig, VariableNamespace};
use vegafusion_core::task_graph::task_value::TaskValue;
use wasm_bindgen::prelude::*;

use js_sys::Promise;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
// use std::sync::mpsc;
use futures::channel::{oneshot, mpsc as async_mpsc};

use wasm_bindgen_futures::{future_to_promise, JsFuture};

use serde_json::Value;
use wasm_bindgen_futures::spawn_local;
use vegafusion_common::data::table::VegaFusionTable;

use vegafusion_core::planning::stitch::CommPlan;
use vegafusion_core::planning::watch::{ExportUpdateJSON, ExportUpdateNamespace, WatchPlan};

use vegafusion_core::proto::gen::services::{
    query_request, query_result, QueryRequest, QueryResult,
};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;

use vegafusion_core::planning::plan::SpecPlan;
use web_sys::Element;
use vegafusion_core::chart_state::ChartState;
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::runtime::VegaFusionRuntimeTrait;

pub fn set_panic_hook() {
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
extern "C" {
    fn alert(s: &str);

    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

pub struct VegaFusionWasmRuntime {
    sender: async_mpsc::Sender<(QueryRequest, oneshot::Sender<vegafusion_common::error::Result<Vec<ResponseTaskValue>>>)>
}

impl VegaFusionWasmRuntime {
    pub fn new(query_fn: js_sys::Function) -> Self {
        let (sender, mut receiver) = async_mpsc::channel::<(QueryRequest, oneshot::Sender<vegafusion_common::error::Result<Vec<ResponseTaskValue>>>)>(32);

        // Spawn a task to process incoming requests
        spawn_local(async move {
            while let Some((request_msg, response_tx)) = receiver.next().await {

                let mut buf: Vec<u8> = Vec::with_capacity(request_msg.encoded_len());
                request_msg.encode(&mut buf).unwrap();
        
                let context =
                    js_sys::JSON::parse(&serde_json::to_string(&serde_json::Value::Null).unwrap()).unwrap();
        
                let js_buffer = js_sys::Uint8Array::from(buf.as_slice());
                let promise = query_fn
                    .call1(&context, &js_buffer)
                    .expect("query_fn function call failed");
                let promise = promise.dyn_into::<Promise>().unwrap();
                let response = JsFuture::from(promise).await.unwrap();
                let response_array = response.dyn_into::<js_sys::Uint8Array>().unwrap();
                let response_bytes = response_array.to_vec();

                let response = QueryResult::decode(response_bytes.as_slice()).unwrap();

                match response.response.unwrap() {
                    query_result::Response::Error(error) => {
                        response_tx.send(Err(vegafusion_common::error::VegaFusionError::internal(format!("{error:?}")))).unwrap();
                    },
                    query_result::Response::TaskGraphValues(task_graph_value_response) => {
                        response_tx.send(Ok(task_graph_value_response.response_values)).unwrap();
                    },
                }
            }
        });

        VegaFusionWasmRuntime {
            sender
        }
    }
}
#[async_trait::async_trait]
impl VegaFusionRuntimeTrait for VegaFusionWasmRuntime {
    async fn query_request(&self, task_graph: Arc<TaskGraph>, indices: &[NodeValueIndex], _inline_datasets: &HashMap<String, VegaFusionDataset>) -> vegafusion_common::error::Result<Vec<ResponseTaskValue>> {
        // Request initial values
        let request_msg = QueryRequest {
            request: Some(query_request::Request::TaskGraphValues(
                TaskGraphValueRequest {
                    task_graph: Some(task_graph.as_ref().clone()),
                    indices: Vec::from(indices),
                    inline_datasets: vec![],  // TODO: inline datasets
                },
            )),
        };

        let (tx, rx) = oneshot::channel();
        self.sender.clone().send((request_msg, tx)).await.unwrap();
        let response = rx.await.unwrap();

        response
    }
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct ChartHandle {
    state: ChartState,
    view: Rc<View>,
    verbose: bool,
    debounce_wait: f64,
    debounce_max_wait: Option<f64>,
    sender: async_mpsc::Sender<ExportUpdateJSON>,
}

#[wasm_bindgen]
impl ChartHandle {
    fn view(&self) -> &View {
        &self.view
    }

    pub fn get_signal(&self, name: &str, scope: &[u32]) -> JsValue {
        get_signal_value(self.view.as_ref(), name, scope)
    }

    pub fn get_data(&self, name: &str, scope: &[u32]) -> JsValue {
        get_data_value(self.view.as_ref(), name, scope)
    }

    pub fn set_signal(&self, name: &str, scope: &[u32], value: JsValue) {
        set_signal_value(self.view.as_ref(), name, scope, value);
    }

    pub fn set_data(&self, name: &str, scope: &[u32], value: JsValue) {
        set_data_value(self.view.as_ref(), name, scope, value);
    }

    pub fn get_state(&self) -> JsValue {
        self.view.get_state()
    }

    pub fn set_state(&self, state: JsValue) {
        self.view.set_state(state)
    }

    pub fn run(&self) {
        self.view.run()
    }

    pub fn run_async(&self) -> Promise {
        self.view.run_async()
    }

    pub fn add_signal_listener(&self, name: &str, scope: &[u32], handler: JsValue) {
        add_signal_listener(
            self.view(),
            name,
            scope,
            handler,
            self.debounce_wait,
            self.debounce_max_wait,
        );
    }

    pub fn add_data_listener(&self, name: &str, scope: &[u32], handler: JsValue) {
        add_data_listener(
            self.view(),
            name,
            scope,
            handler,
            self.debounce_wait,
            self.debounce_max_wait,
        );
    }

    fn update_view(&self, updates: &[ExportUpdateJSON]) {
        for update in updates {
            match update.namespace {
                ExportUpdateNamespace::Signal => {
                    let js_value =
                        js_sys::JSON::parse(&serde_json::to_string(&update.value).unwrap())
                            .unwrap();
                    self.set_signal(&update.name, update.scope.as_slice(), js_value);
                }
                ExportUpdateNamespace::Data => {
                    let js_value =
                        js_sys::JSON::parse(&serde_json::to_string(&update.value).unwrap())
                            .unwrap();
                    self.set_data(&update.name, update.scope.as_slice(), js_value);
                }
            }
        }
    }

    pub fn register_callbacks(&self) {
        for scoped_var in &self.state.get_comm_plan().client_to_server {
            let var_name = scoped_var.0.name.clone();
            let scope = Vec::from(scoped_var.1.as_slice());

            let sender = self.sender.clone();
            let verbose = self.verbose;
            // let this = self.clone();
            match scoped_var.0.namespace() {
                VariableNamespace::Signal => {
                    let closure = Closure::wrap(Box::new(move |name: String, val: JsValue| {
                        let mut sender = sender.clone();
                        let val: Value = if val.is_undefined() {
                            Value::Null
                        } else {
                            serde_json::from_str(
                                &js_sys::JSON::stringify(&val).unwrap().as_string().unwrap(),
                            )
                                .unwrap()
                        };

                        if verbose {
                            log(&format!("VegaFusion(wasm): Sending signal {name}"));
                            log(&serde_json::to_string_pretty(&val).unwrap());
                        }

                        let update = ExportUpdateJSON {
                            namespace: ExportUpdateNamespace::Signal,
                            name,
                            scope: scope.clone(),
                            value: val,
                        };
                        spawn_local(async move {
                            sender.send(update).await.unwrap();
                        });
                    })
                        as Box<dyn FnMut(String, JsValue)>);

                    let ret_cb = closure.as_ref().clone();
                    closure.forget();

                    self.add_signal_listener(&var_name, scoped_var.1.as_slice(), ret_cb);
                }
                VariableNamespace::Data => {
                    let closure = Closure::wrap(Box::new(move |name: String, val: JsValue| {
                        let mut sender = sender.clone();
                        let val: serde_json::Value = serde_json::from_str(
                            &js_sys::JSON::stringify(&val).unwrap().as_string().unwrap(),
                        )
                            .unwrap();
                        if verbose {
                            log(&format!("VegaFusion(wasm): Sending data {name}"));
                            log(&serde_json::to_string_pretty(&val).unwrap());
                        }

                        let update = ExportUpdateJSON {
                            namespace: ExportUpdateNamespace::Data,
                            name,
                            scope: scope.clone(),
                            value: val,
                        };
                        spawn_local(async move {
                            sender.send(update).await.unwrap();
                        });
                    })
                        as Box<dyn FnMut(String, JsValue)>);

                    let ret_cb = closure.as_ref().clone();
                    closure.forget();

                    self.add_data_listener(&var_name, scoped_var.1.as_slice(), ret_cb);
                }
                VariableNamespace::Scale => {}
            }
        }
    }

    pub fn client_spec_json(&self) -> String {
        serde_json::to_string_pretty(self.state.get_client_spec()).unwrap()
    }

    pub fn server_spec_json(&self) -> String {
        serde_json::to_string_pretty(self.state.get_server_spec()).unwrap()
    }

    pub fn comm_plan_json(&self) -> String {
        serde_json::to_string_pretty(&WatchPlan::from(self.state.get_comm_plan().clone())).unwrap()
    }

    pub fn to_image_url(&self, img_type: &str, scale_factor: Option<f64>) -> Promise {
        self.view
            .to_image_url(img_type, scale_factor.unwrap_or(1.0))
    }
}

#[wasm_bindgen]
pub async fn render_vegafusion(
    element: Element,
    spec_str: &str,
    verbose: bool,
    debounce_wait: f64,
    debounce_max_wait: Option<f64>,
    query_fn: js_sys::Function,
) -> ChartHandle {
    set_panic_hook();
    let spec: ChartSpec = serde_json::from_str(spec_str).unwrap();

    let local_tz = local_timezone();
    let tz_config = TzConfig {
        local_tz,
        default_input_tz: None,
    };

    let runtime = VegaFusionWasmRuntime::new(query_fn);
    let chart_state = ChartState::try_new(&runtime, spec, Default::default(), tz_config, None).await.unwrap();
    // Mount vega chart
    let dataflow = parse(
        js_sys::JSON::parse(
            &serde_json::to_string(chart_state.get_transformed_spec()).expect("Failed to parse spec as JSON"),
        )
            .unwrap(),
    );

    let view = View::new(dataflow);
    view.initialize(element);
    view.hover();
    setup_tooltip(&view);

    let (sender, mut receiver) = async_mpsc::channel::<ExportUpdateJSON>(16);

    let view_rc = Rc::new(view);
    let handle = ChartHandle {
        state: chart_state, view: view_rc.clone(), verbose, debounce_wait, debounce_max_wait, sender
    };
    handle.register_callbacks();
    let inner_handle = handle.clone();
    
    // listen for callback updates
    spawn_local(async move {
        while let Some(update) = receiver.next().await {
            let response_update = inner_handle.state.update(&runtime, vec![update]).await.unwrap();
            inner_handle.update_view(&response_update);
        }
    });

    view_rc.run();

    handle
}


#[wasm_bindgen]
pub fn vega_version() -> String {
    inner_vega_version()
}

#[wasm_bindgen]
pub fn make_grpc_send_message_fn(client: JsValue, hostname: String) -> js_sys::Function {
    inner_make_grpc_send_message_fn(client, hostname)
}

#[wasm_bindgen(module = "/js/vega_utils.js")]
extern "C" {
    #[wasm_bindgen(js_name = "vega_version")]
    fn inner_vega_version() -> String;

    #[wasm_bindgen(js_name = "localTimezone")]
    fn local_timezone() -> String;

    #[wasm_bindgen(js_name = "make_grpc_send_message_fn")]
    fn inner_make_grpc_send_message_fn(client: JsValue, hostname: String) -> js_sys::Function;

    #[wasm_bindgen(js_name = "getSignalValue")]
    fn get_signal_value(view: &View, name: &str, scope: &[u32]) -> JsValue;

    #[wasm_bindgen(js_name = "setSignalValue")]
    fn set_signal_value(view: &View, name: &str, scope: &[u32], value: JsValue);

    #[wasm_bindgen(js_name = "getDataValue")]
    fn get_data_value(view: &View, name: &str, scope: &[u32]) -> JsValue;

    #[wasm_bindgen(js_name = "setDataValue")]
    pub fn set_data_value(view: &View, name: &str, scope: &[u32], value: JsValue);

    #[wasm_bindgen(js_name = "addSignalListener")]
    fn add_signal_listener(
        view: &View,
        name: &str,
        scope: &[u32],
        handler: JsValue,
        wait: f64,
        maxWait: Option<f64>,
    );

    #[wasm_bindgen(js_name = "addDataListener")]
    fn add_data_listener(
        view: &View,
        name: &str,
        scope: &[u32],
        handler: JsValue,
        wait: f64,
        maxWait: Option<f64>,
    );

    #[wasm_bindgen(js_name = "setupTooltip")]
    fn setup_tooltip(view: &View);
}

#[wasm_bindgen(module = "vega")]
extern "C" {
    pub fn parse(spec: JsValue) -> JsValue;

    pub type View;

    #[wasm_bindgen(constructor)]
    pub fn new(dataflow: JsValue) -> View;

    #[wasm_bindgen(method, js_name = "initialize")]
    pub fn initialize(this: &View, container: Element);

    #[wasm_bindgen(method, js_name = "run")]
    pub fn run(this: &View);

    #[wasm_bindgen(method, js_name = "runAsync")]
    pub fn run_async(this: &View) -> Promise;

    #[wasm_bindgen(method, js_name = "hover")]
    pub fn hover(this: &View);

    #[wasm_bindgen(method, js_name = "getState")]
    pub fn get_state(this: &View) -> JsValue;

    #[wasm_bindgen(method, js_name = "setState")]
    pub fn set_state(this: &View, state: JsValue);

    #[wasm_bindgen(method, js_name = "toImageURL")]
    pub fn to_image_url(this: &View, img_type: &str, scale_factor: f64) -> Promise;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("it works");
    }
}
