use deno_core::JsRuntime;
use deno_core::{
    op_sync, resolve_import, ModuleLoader, ModuleSource, ModuleSourceFuture,
    ModuleSpecifier, RuntimeOptions,
};

use deno_core::error::AnyError;
use deno_core::futures::FutureExt;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use vega_fusion::expression::ast::base::Expression;

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

pub struct VegaJsRuntime {
    runtime: Arc<Mutex<JsRuntime>>,
    input_val: Arc<Mutex<HashMap<String, String>>>,
}

impl VegaJsRuntime {
    pub fn new() -> Self {
        // Initialize a runtime instance with UrlModuleLoader
        let runtime_opts = RuntimeOptions {
            module_loader: Some(Rc::new(UrlModuleLoader::new())),
            ..Default::default()
        };
        let mut runtime = JsRuntime::new(runtime_opts);

        // Register an op for string input value
        let input_val = Arc::new(Mutex::new(HashMap::<String, String>::new()));

        let closure_in_val = input_val.clone();
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

        Self {
            runtime: Arc::new(Mutex::new(runtime)),
            input_val,
        }
    }

    pub fn set_input_value(&self, key: &str, value: String) {
        let mut locked = self.input_val.lock().unwrap();
        locked.insert(key.to_string(), value);
    }

    pub fn parse_expression(&self, expr: &str) -> Expression {
        self.set_input_value("expr_str", expr.to_string());

        // Now see if vega_global is available next time
        let mut locked_runtime = self.runtime.lock();
        let runtime = locked_runtime.as_mut().unwrap();

        let result = runtime
            .execute_script(
                "<usage>",
                r#"
let expr_str = Deno.core.opSync('inputs', 'expr_str');
let expr = vega.parseExpression(expr_str);
JSON.stringify(expr)
"#,
            )
            .unwrap();

        let scope = &mut runtime.handle_scope();
        let value = result.get(scope);

        // Convert return to string and deserialize to JSON
        let value_string = value.to_rust_string_lossy(scope);
        serde_json::from_str(&value_string).unwrap()
    }

    pub fn eval_scalar_expression(
        &self,
        expr: &str,
        scope: &HashMap<String, serde_json::Value>,
    ) -> serde_json::Value {
        self.set_input_value("expr_str", expr.to_string());
        self.set_input_value("scope", serde_json::to_string(scope).unwrap());

        // Now see if vega_global is available next time
        let mut locked_runtime = self.runtime.lock();
        let runtime = locked_runtime.as_mut().unwrap();

        let result = runtime
            .execute_script(
                "<usage>",
                r#"
let expr_str = Deno.core.opSync('inputs', 'expr_str');
let scope = JSON.parse(Deno.core.opSync('inputs', 'scope'));
let expr = vega.parseExpression(expr_str);
let code = codegen(expr).code;

let func = Function("_", "'use strict'; return " + code).bind(vega_functions.functionContext);
JSON.stringify(func(scope))
"#,
            )
            .unwrap();

        let scope = &mut runtime.handle_scope();
        let value = result.get(scope);

        // Convert return to string and deserialize to JSON
        let value_string = value.to_rust_string_lossy(scope);
        serde_json::from_str(&value_string).unwrap()
    }
}
