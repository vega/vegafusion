use futures::{SinkExt, StreamExt};
use prost::Message;

use serde::{Deserialize, Serialize};
use vegafusion_core::proto::gen::tasks::{
    NodeValueIndex, ResponseTaskValue, TaskGraph, TaskGraphValueRequest, TzConfig,
    VariableNamespace,
};
use vegafusion_core::task_graph::task_value::NamedTaskValue;
use wasm_bindgen::prelude::*;

use futures::channel::{mpsc as async_mpsc, oneshot};
use js_sys::Promise;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use wasm_bindgen_futures::JsFuture;

use serde_json::{json, Value};
use wasm_bindgen_futures::spawn_local;

use vegafusion_core::planning::watch::{ExportUpdateJSON, ExportUpdateNamespace, WatchPlan};

use vegafusion_core::proto::gen::services::{
    query_request, query_result, QueryRequest, QueryResult,
};
use vegafusion_core::runtime::VegaFusionRuntimeTrait;
use vegafusion_core::spec::chart::ChartSpec;

use vegafusion_core::chart_state::{ChartState, ChartStateOpts};
use vegafusion_core::data::dataset::VegaFusionDataset;
use vegafusion_core::get_column_usage;
use vegafusion_runtime::task_graph::runtime::{encode_inline_datasets, VegaFusionRuntime};
use web_sys::Element;

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
extern "C" {
    fn alert(s: &str);

    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

/// VegaFusionRuntimeTrait implementation that sends query requests to a VegaFusionRuntime using
/// an async JavaScript function.
pub struct QueryFnVegaFusionRuntime {
    sender: async_mpsc::Sender<(
        QueryRequest,
        oneshot::Sender<vegafusion_common::error::Result<Vec<ResponseTaskValue>>>,
    )>,
}

impl QueryFnVegaFusionRuntime {
    pub fn new(query_fn: js_sys::Function) -> Self {
        let (sender, mut receiver) = async_mpsc::channel::<(
            QueryRequest,
            oneshot::Sender<vegafusion_common::error::Result<Vec<ResponseTaskValue>>>,
        )>(32);

        // Spawn a task to process incoming requests
        spawn_local(async move {
            while let Some((request_msg, response_tx)) = receiver.next().await {
                let mut buf: Vec<u8> = Vec::with_capacity(request_msg.encoded_len());
                request_msg.encode(&mut buf).unwrap();

                let context = JsValue::null();

                let js_buffer = js_sys::Uint8Array::from(buf.as_slice());
                let promise = match query_fn.call1(&context, &js_buffer) {
                    Ok(p) => p,
                    Err(e) => {
                        response_tx
                            .send(Err(vegafusion_common::error::VegaFusionError::internal(
                                format!(
                                    "Failed to call send query functions: {}",
                                    js_sys::JSON::stringify(&e).unwrap()
                                ),
                            )))
                            .unwrap();
                        continue;
                    }
                };
                let promise = match promise.dyn_into::<Promise>() {
                    Ok(p) => p,
                    Err(e) => {
                        response_tx
                            .send(Err(vegafusion_common::error::VegaFusionError::internal(
                                format!(
                                    "send query function did not return a promise: {}",
                                    js_sys::JSON::stringify(&e).unwrap()
                                ),
                            )))
                            .unwrap();
                        continue;
                    }
                };
                let response = match JsFuture::from(promise).await {
                    Ok(response) => response,
                    Err(e) => {
                        response_tx.send(Err(vegafusion_common::error::VegaFusionError::internal(
                            format!("Error when resolving promise returned by send query function: {}", js_sys::JSON::stringify(&e).unwrap())
                        ))).unwrap();
                        continue;
                    }
                };
                let response_array = match response.dyn_into::<js_sys::Uint8Array>() {
                    Ok(response_array) => response_array,
                    Err(e) => {
                        response_tx
                            .send(Err(vegafusion_common::error::VegaFusionError::internal(
                                format!(
                                    "send query function did not return a Uint8Array: {}",
                                    js_sys::JSON::stringify(&e).unwrap()
                                ),
                            )))
                            .unwrap();
                        continue;
                    }
                };

                let response_bytes = response_array.to_vec();

                let response = QueryResult::decode(response_bytes.as_slice()).unwrap();

                match response.response.unwrap() {
                    query_result::Response::Error(error) => {
                        response_tx
                            .send(Err(vegafusion_common::error::VegaFusionError::internal(
                                format!("{error:?}"),
                            )))
                            .unwrap();
                    }
                    query_result::Response::TaskGraphValues(task_graph_value_response) => {
                        response_tx
                            .send(Ok(task_graph_value_response.response_values))
                            .unwrap();
                    }
                }
            }
        });

        QueryFnVegaFusionRuntime { sender }
    }
}

#[async_trait::async_trait]
impl VegaFusionRuntimeTrait for QueryFnVegaFusionRuntime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn query_request(
        &self,
        task_graph: Arc<TaskGraph>,
        indices: &[NodeValueIndex],
        inline_datasets: &HashMap<String, VegaFusionDataset>,
    ) -> vegafusion_common::error::Result<Vec<NamedTaskValue>> {
        // Request initial values
        let request_msg = QueryRequest {
            request: Some(query_request::Request::TaskGraphValues(
                TaskGraphValueRequest {
                    task_graph: Some(task_graph.as_ref().clone()),
                    indices: Vec::from(indices),
                    inline_datasets: encode_inline_datasets(inline_datasets)?,
                },
            )),
        };

        let (tx, rx) = oneshot::channel();
        self.sender.clone().send((request_msg, tx)).await.unwrap();
        let response = rx.await.unwrap()?;

        Ok(response
            .into_iter()
            .map(|v| v.into())
            .collect::<Vec<NamedTaskValue>>())
    }
}

#[wasm_bindgen]
#[derive(Clone)]
/// Handle to an embedded VegaFusion chart
pub struct ChartHandle {
    state: ChartState,
    embed: Rc<EmbedResult>,
    verbose: bool,
    debounce_wait: f64,
    debounce_max_wait: Option<f64>,
    sender: async_mpsc::Sender<ExportUpdateJSON>,
}

#[wasm_bindgen]
impl ChartHandle {
    /// Get the Vega-Embed view
    /// @returns The Vega-Embed view
    pub fn view(&self) -> View {
        self.embed.view()
    }

    /// Finalize the chart
    pub fn finalize(&self) {
        self.embed.finalize()
    }

    /// Get the client specification
    /// @returns The client specification
    #[wasm_bindgen(js_name = clientSpec)]
    pub fn client_spec(&self) -> JsValue {
        self.state
            .get_client_spec()
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .unwrap()
    }

    /// Get the server specification
    /// @returns The server specification
    #[wasm_bindgen(js_name = serverSpec)]
    pub fn server_spec(&self) -> JsValue {
        self.state
            .get_server_spec()
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .unwrap()
    }

    /// Get the communication plan
    /// @returns The communication plan
    #[wasm_bindgen(js_name = commPlan)]
    pub fn comm_plan(&self) -> JsValue {
        WatchPlan::from(self.state.get_comm_plan().clone())
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .unwrap()
    }

    fn register_callbacks(&self) {
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
                            serde_wasm_bindgen::from_value(val)
                                .expect("Failed to convert JsValue to Value")
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

                    add_signal_listener(
                        &self.view(),
                        &var_name,
                        scoped_var.1.as_slice(),
                        ret_cb,
                        self.debounce_wait,
                        self.debounce_max_wait,
                    );
                }
                VariableNamespace::Data => {
                    let closure = Closure::wrap(Box::new(move |name: String, val: JsValue| {
                        let mut sender = sender.clone();
                        let val: Value = serde_wasm_bindgen::from_value(val)
                            .expect("Failed to convert JsValue to Value");

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

                    add_data_listener(
                        &self.view(),
                        &var_name,
                        scoped_var.1.as_slice(),
                        ret_cb,
                        self.debounce_wait,
                        self.debounce_max_wait,
                    );
                }
                VariableNamespace::Scale => {}
            }
        }
    }

    fn update_view(&self, updates: &[ExportUpdateJSON]) {
        for update in updates {
            match update.namespace {
                ExportUpdateNamespace::Signal => {
                    let js_value = update
                        .value
                        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
                        .unwrap();
                    set_signal_value(
                        &self.view(),
                        &update.name,
                        update.scope.as_slice(),
                        js_value,
                    );
                }
                ExportUpdateNamespace::Data => {
                    let js_value = update
                        .value
                        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
                        .unwrap();
                    set_data_value(
                        &self.view(),
                        &update.name,
                        update.scope.as_slice(),
                        js_value,
                    );
                }
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct VegaFusionEmbedConfig {
    #[serde(default = "default_verbose")]
    verbose: bool,
    #[serde(default = "default_debounce_wait")]
    debounce_wait: f64,
    #[serde(default)]
    debounce_max_wait: Option<f64>,
    #[serde(default = "default_embed_opts")]
    embed_opts: Value,
}

fn default_verbose() -> bool {
    false
}

fn default_debounce_wait() -> f64 {
    30.0
}

fn default_embed_opts() -> Value {
    json!({"mode": "vega"})
}

impl Default for VegaFusionEmbedConfig {
    fn default() -> Self {
        VegaFusionEmbedConfig {
            verbose: default_verbose(),
            debounce_wait: default_debounce_wait(),
            debounce_max_wait: None,
            embed_opts: default_embed_opts(),
        }
    }
}

/// Embed a Vega chart and accelerate with VegaFusion
/// @param element - The DOM element to embed the visualization into
/// @param spec - The Vega specification (as string or object)
/// @param config - Optional configuration options
/// @param query_fn - Function to handle server-side query requests.
///     If not provided, an embedded wasm VegaFusion runtime is created.
/// @returns A ChartHandle instance for the embedded visualization
#[wasm_bindgen(js_name = vegaFusionEmbed)]
pub async fn vegafusion_embed(
    element: Element,
    spec: JsValue,
    config: JsValue,
    query_fn: JsValue,
) -> Result<ChartHandle, JsValue> {
    set_panic_hook();
    let spec: ChartSpec = if spec.is_string() {
        serde_json::from_str(&spec.as_string().unwrap())
            .map_err(|_e| JsError::new("Failed to convert JsValue to ChartSpec"))?
    } else {
        serde_wasm_bindgen::from_value(spec)
            .map_err(|_e| JsError::new("Failed to convert JsValue to ChartSpec"))?
    };

    let config: VegaFusionEmbedConfig = if config.is_undefined() || config.is_null() {
        VegaFusionEmbedConfig::default()
    } else {
        serde_wasm_bindgen::from_value(config)
            .map_err(|_e| JsError::new("Failed to convert JsValue to VegaFusionEmbedConfig"))?
    };

    let local_tz = local_timezone();
    let tz_config = TzConfig {
        local_tz,
        default_input_tz: None,
    };

    let runtime: Box<dyn VegaFusionRuntimeTrait> = if query_fn.is_undefined() || query_fn.is_null()
    {
        // Use embedded runtime
        Box::new(VegaFusionRuntime::default())
    } else {
        let query_fn = query_fn.dyn_into::<js_sys::Function>().map_err(|e| {
            JsError::new(&format!(
                "Expected query_fn to be a Function: {}",
                js_sys::JSON::stringify(&e).unwrap()
            ))
        })?;
        Box::new(QueryFnVegaFusionRuntime::new(query_fn))
    };

    let chart_state = ChartState::try_new(
        runtime.as_ref(),
        spec,
        Default::default(),
        ChartStateOpts {
            tz_config,
            row_limit: None,
        },
    )
    .await
    .map_err(|e| JsError::new(&e.to_string()))?;

    // Serializer that can be used to convert serde types to JSON compatible objects
    let serializer = serde_wasm_bindgen::Serializer::json_compatible();

    // Render Vega chart with vega-embed
    let spec_value = chart_state
        .get_transformed_spec()
        .serialize(&serializer)
        .expect("Failed to convert spec to JsValue");

    // Add vega-embed options
    let opts = config
        .embed_opts
        .serialize(&serializer)
        .expect("Failed to convert embed_opts to JsValue");

    let embed = embed(element, spec_value, opts).await.map_err(|e| {
        JsError::new(&format!(
            "Failed to embed chart: {}",
            js_sys::JSON::stringify(&e).unwrap()
        ))
    })?;

    let (sender, mut receiver) = async_mpsc::channel::<ExportUpdateJSON>(16);

    let handle = ChartHandle {
        state: chart_state,
        embed: Rc::new(embed),
        verbose: config.verbose,
        debounce_wait: config.debounce_wait,
        debounce_max_wait: config.debounce_max_wait,
        sender,
    };

    handle.register_callbacks();
    let inner_handle = handle.clone();

    // listen for callback updates
    spawn_local(async move {
        while let Some(update) = receiver.next().await {
            if let Ok(response_update) = inner_handle
                .state
                .update(runtime.as_ref(), vec![update])
                .await
            {
                inner_handle.update_view(&response_update);
            }
        }
    });

    Ok(handle)
}

#[wasm_bindgen(js_name = "getColumnUsage")]
pub fn wasm_get_column_usage(spec: JsValue) -> Result<JsValue, JsValue> {
    let spec: ChartSpec = if spec.is_string() {
        serde_json::from_str(&spec.as_string().unwrap())
            .map_err(|_e| JsError::new("Failed to convert JsValue to ChartSpec"))?
    } else {
        serde_wasm_bindgen::from_value(spec)
            .map_err(|_e| JsError::new("Failed to convert JsValue to ChartSpec"))?
    };
    let usage = get_column_usage(&spec).map_err(|_e| JsError::new("Failed to get column usage"))?;

    Ok(usage
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|_e| JsError::new("Failed to serialize column usage"))?)
}

/// Create a function for sending VegaFusion queries to VegaFusion server over gRPC-Web
/// @param client - The gRPC client instance
/// @param hostname - The hostname to connect to
/// @returns A function that can be used to send gRPC messages
#[wasm_bindgen(js_name = "makeGrpcSendMessageFn")]
pub fn make_grpc_send_message_fn(client: JsValue, hostname: String) -> js_sys::Function {
    inner_make_grpc_send_message_fn(client, hostname)
}

#[wasm_bindgen(module = "/js/vega_utils.js")]
extern "C" {
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
    fn set_data_value(view: &View, name: &str, scope: &[u32], value: JsValue);

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
}

#[wasm_bindgen(module = "vega-embed")]
extern "C" {
    type EmbedResult;

    #[wasm_bindgen(catch, js_name = "default")]
    pub async fn embed(el: Element, spec: JsValue, opt: JsValue) -> Result<EmbedResult, JsValue>;

    #[wasm_bindgen(method, getter)]
    fn view(this: &EmbedResult) -> View;

    #[wasm_bindgen(method, getter)]
    fn spec(this: &EmbedResult) -> JsValue;

    #[wasm_bindgen(method, getter)]
    fn vgSpec(this: &EmbedResult) -> JsValue;

    #[wasm_bindgen(method)]
    fn finalize(this: &EmbedResult);

    // View
    pub type View;

    #[wasm_bindgen(method, js_name = "run")]
    fn run(this: &View);

    #[wasm_bindgen(method, js_name = "runAsync")]
    fn run_async(this: &View) -> Promise;

    #[wasm_bindgen(method, js_name = "getState")]
    fn get_state(this: &View) -> JsValue;

    #[wasm_bindgen(method, js_name = "setState")]
    fn set_state(this: &View, state: JsValue);

    #[wasm_bindgen(method, js_name = "toImageURL")]
    fn to_image_url(this: &View, img_type: &str, scale_factor: f64) -> Promise;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("it works");
    }
}
