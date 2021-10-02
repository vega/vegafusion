use deno_core::JsRuntime;
use deno_core::{
    op_sync, resolve_import, ModuleLoader, ModuleSource, ModuleSourceFuture, ModuleSpecifier,
    RuntimeOptions,
};

use deno_core::error::AnyError;
use deno_core::futures::FutureExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use vega_fusion::expression::ast::base::Expression;

use futures::channel::{mpsc, mpsc::Sender, oneshot};
use futures::executor::block_on;
use futures_util::{SinkExt, StreamExt};
use std::thread;

/// Modification of the FsModuleLoader to use reqwest to load modules from URLs
struct UrlModuleLoader {
    cache: Arc<Mutex<HashMap<String, ModuleSource>>>,
}
impl UrlModuleLoader {
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl ModuleLoader for UrlModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        referrer: &str,
        _is_main: bool,
    ) -> Result<ModuleSpecifier, AnyError> {
        Ok(resolve_import(specifier, referrer)?)
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<ModuleSpecifier>,
        _is_dynamic: bool,
    ) -> Pin<Box<ModuleSourceFuture>> {
        let module_specifier = module_specifier.clone();
        let cache = self.cache.clone();
        async move {
            let url = module_specifier.to_string();
            let mut locked = cache.lock();
            let mut_cache = locked.as_mut().unwrap();
            let module = if let Some(module) = mut_cache.get(&url) {
                module.clone()
            } else {
                let code = reqwest::blocking::get(&url).unwrap().text().unwrap();
                let module = ModuleSource {
                    code,
                    module_url_specified: module_specifier.to_string(),
                    module_url_found: module_specifier.to_string(),
                };

                mut_cache.insert(url, module.clone());
                module
            };
            Ok(module)
        }
        .boxed_local()
    }
}

#[derive(Clone)]
pub struct VegaJsRuntime {
    runtime_manager: Arc<thread::JoinHandle<()>>,
    command_sender: Sender<JsRuntimeCommand>,
}

impl VegaJsRuntime {
    pub fn new() -> Self {
        let (command_sender, mut command_receiver) = mpsc::channel::<JsRuntimeCommand>(32);
        let runtime_manager = thread::spawn(move || {
            // # Initialize JsRuntime in the manager thread

            // Initialize a runtime instance with UrlModuleLoader
            let mut runtime = JsRuntime::new(RuntimeOptions {
                module_loader: Some(Rc::new(UrlModuleLoader::new())),
                ..Default::default()
            });

            // Register an op for string input value
            let all_inputs = Arc::new(Mutex::new(HashMap::<String, String>::new()));

            let closure_in_val = all_inputs.clone();
            runtime.register_op(
                "inputs",
                // The op-layer automatically deserializes inputs
                // and serializes the returned Result & value
                op_sync(move |_state, args: String, _: ()| {
                    let locked = closure_in_val.lock().unwrap();
                    Ok(locked.get(&args).unwrap().clone())
                }),
            );

            // Save ops
            runtime.sync_ops_cache();

            // Execute script (and wait for futures to resolve) that import external modules and
            // assigns them to top-level variables.
            runtime
                .execute_script(
                    "<imports>",
                    r#"
var vega;
var vega_functions;
var codegen;

import('https://cdn.skypack.dev/vega').then((imported) => {
    vega = imported;
    import('https://cdn.skypack.dev/vega-functions').then((imported) => {
        vega_functions = imported;
        codegen = vega.codegenExpression(vega_functions.codegenParams);
    })
})
"#,
                )
                .unwrap();

            futures::executor::block_on(runtime.run_event_loop(false)).unwrap();

            // Receiving and process messages
            while let Some(cmd) = block_on(command_receiver.next()) {
                match cmd {
                    JsRuntimeCommand::ExecuteScript {
                        script,
                        inputs,
                        responder,
                    } => {
                        // Update inputs with those provided in the message
                        {
                            let mut locked_inputs = all_inputs.lock().unwrap();
                            for (k, v) in inputs {
                                locked_inputs.insert(k, v);
                            }
                        }

                        // Execute script and retrieve return value string
                        let value_string = {
                            let result = runtime.execute_script("<usage>", &script).unwrap();

                            let scope = &mut runtime.handle_scope();
                            let value = result.get(scope).clone();
                            value.to_rust_string_lossy(scope)
                        };

                        responder.send(value_string).unwrap();
                    }
                }
            }
        });

        Self {
            runtime_manager: Arc::new(runtime_manager),
            command_sender,
        }
    }

    fn execute_script(&mut self, script: &str, inputs: &HashMap<String, String>) -> String {
        let (resp_tx, resp_rx) = oneshot::channel::<String>();
        let cmd = JsRuntimeCommand::ExecuteScript {
            script: script.to_string(),
            inputs: inputs.clone(),
            responder: resp_tx,
        };

        // Send request
        block_on(self.command_sender.send(cmd)).unwrap();

        // Wait for result
        futures::executor::block_on(resp_rx).unwrap()
    }

    pub fn parse_expression(&mut self, expr: &str) -> Expression {
        let script = r#"
(() => {
    let expr_str = Deno.core.opSync('inputs', 'expr_str');
    let expr = vega.parseExpression(expr_str);
    return JSON.stringify(expr)
})()
"#;

        let inputs: HashMap<_, _> = vec![("expr_str".to_string(), expr.to_string())]
            .into_iter()
            .collect();

        let value_string = self.execute_script(script, &inputs);
        serde_json::from_str(&value_string).unwrap()
    }

    pub fn eval_scalar_expression(
        &mut self,
        expr: &str,
        scope: &HashMap<String, serde_json::Value>,
    ) -> serde_json::Value {
        let script = r#"
(() => {
    let expr_str = Deno.core.opSync('inputs', 'expr_str');
    let scope = JSON.parse(Deno.core.opSync('inputs', 'scope'));
    let expr = vega.parseExpression(expr_str);
    let code = codegen(expr).code;

    let func = Function("_", "'use strict'; return " + code).bind(vega_functions.functionContext);
    return JSON.stringify(func(scope))
})()
"#;
        let inputs: HashMap<_, _> = vec![
            ("expr_str".to_string(), expr.to_string()),
            ("scope".to_string(), serde_json::to_string(scope).unwrap()),
        ]
        .into_iter()
        .collect();

        let value_string = self.execute_script(script, &inputs);
        serde_json::from_str(&value_string).unwrap()
    }
}

pub enum JsRuntimeCommand {
    ExecuteScript {
        script: String,
        inputs: HashMap<String, String>,
        responder: oneshot::Sender<String>,
    },
}

lazy_static! {
    static ref VEGA_JS_RUNTIME: Mutex<VegaJsRuntime> = Mutex::new(VegaJsRuntime::new());
}
pub fn vegajs_runtime() -> MutexGuard<'static, VegaJsRuntime> {
    VEGA_JS_RUNTIME.lock().unwrap()
}
