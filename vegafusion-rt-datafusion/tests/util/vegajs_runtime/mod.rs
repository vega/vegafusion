use datafusion::scalar::ScalarValue;
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::ops::Deref;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
// use vega_fusion::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, ToInternalError, VegaFusionError};
// use vega_fusion::expression::compiler::config::CompilationConfig;
// use vega_fusion::expression::compiler::utils::ScalarValueHelpers;
// use vega_fusion::spec::transform::TransformSpec;
use self::super::estree_expression::ESTreeExpression;
use vegafusion_core::proto::gen::expression::Expression;
use vegafusion_core::spec::transform::TransformSpec;
use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::data::table::VegaFusionTable;
use itertools::Itertools;

lazy_static! {
    static ref UNDEFINED_RE: Regex = Regex::new(r"\bundefined\b").unwrap();
}

pub struct NodeJsRuntime {
    proc: Mutex<Child>,
    out: Arc<Mutex<Vec<u8>>>,
}

impl NodeJsRuntime {
    pub fn try_new() -> Result<Self> {
        // Compute directory of node_modules and vegajsRuntime.js script
        let mut working_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        working_dir.push("tests");
        working_dir.push("util");
        working_dir.push("vegajs_runtime");

        println!("crate_dir: {}", working_dir.to_str().unwrap());

        let mut proc = Command::new("nodejs")
            .args(&["-i", "--experimental-repl-await"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(working_dir)
            .spawn()
            .internal("Failed to launch nodejs")?;

        let out = Self::child_stream_to_vec(
            proc.stdout
                .take()
                .with_context(|| "Failed to create stdout stream".to_string())?,
        );

        let this = Self {
            proc: Mutex::new(proc),
            out,
        };
        this.execute_statement("VegaUtils = require('./vegajsRuntime.js')")
            .unwrap();

        Ok(this)
    }

    /// Pipe streams are blocking, we need separate threads to monitor them without blocking the primary thread.
    /// Credit: https://stackoverflow.com/questions/34611742/how-do-i-read-the-output-of-a-child-process-without-blocking-in-rust
    fn child_stream_to_vec<R>(mut stream: R) -> Arc<Mutex<Vec<u8>>>
    where
        R: Read + Send + 'static,
    {
        let out = Arc::new(Mutex::new(Vec::new()));
        let vec = out.clone();
        thread::Builder::new()
            .name("child_stream_to_vec".into())
            .spawn(move || loop {
                let mut buf = [0];
                match stream.read(&mut buf) {
                    Err(err) => {
                        println!("{}] Error reading from stream: {}", line!(), err);
                        break;
                    }
                    Ok(got) => {
                        if got == 0 {
                            break;
                        } else if got == 1 {
                            vec.lock().expect("!lock").push(buf[0])
                        } else {
                            println!("{}] Unexpected number of bytes: {}", line!(), got);
                            break;
                        }
                    }
                }
            })
            .expect("!thread");
        out
    }

    /// Execute a statement and return and resulting output as a string
    pub fn execute_statement(&self, statement: &str) -> Result<String> {
        // lock process mutex to start, and hold it until after reading the result from standard
        // out to avoid race condition
        let mut locked = self
            .proc
            .lock()
            .internal("Failed to acquire lock to nodejs process")?;
        let process_stdin = locked.stdin.as_mut().unwrap();

        // Maybe add a newline to statement
        let mut statement = statement.to_string();
        if !&statement.ends_with('\n') {
            statement.push('\n');
        }

        process_stdin
            .write_all(statement.as_bytes())
            .expect("Couldn't write");
        process_stdin.flush().unwrap();

        let boundary = "\n> ".as_bytes();
        let bytes_read = loop {
            let mut vec = self.out.deref().lock().unwrap();
            let n = vec.len() as i32;
            if n >= 3 && &vec[(n - 3) as usize..] == boundary {
                let cloned = vec.clone();
                vec.clear();
                break cloned;
            }
        };

        // Maybe a leading prompt
        let start_index = if bytes_read[..2] == boundary[1..] {
            2
        } else {
            0
        };
        // Definitely a trailing prompt
        let end_index = bytes_read.len() - 3;

        let s = String::from_utf8(Vec::from(&bytes_read[start_index..end_index])).unwrap();

        Ok(s.trim().to_string())
    }
}

#[derive(Clone)]
pub struct VegaJsRuntime {
    nodejs_runtime: Arc<NodeJsRuntime>,
}

impl VegaJsRuntime {
    fn new(nodejs_runtime: Arc<NodeJsRuntime>) -> Self {
        Self { nodejs_runtime }
    }

    /// Convert JSON string, as escaped and printed to the console, to one that serde can handle
    fn clean_json_string(json_string: &str) -> String {
        // Remove outer quotes
        let json_string = &json_string[1..json_string.len() - 1];

        // Remove double escapes before double quotes
        let json_string = json_string.replace(r#"\\""#, r#"\""#);

        // Remove escapes before single quotes
        let json_string = json_string.replace(r#"\'"#, r#"'"#);

        // Replace 'undefined' with null so JSON parser can handle it
        let json_string = UNDEFINED_RE.replace(&json_string, "null").into_owned();

        json_string
    }

    fn clean_and_parse_json<T>(json_str: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let json_str = Self::clean_json_string(json_str);
        match serde_json::from_str(&json_str) {
            Err(_err) => {
                return Err(VegaFusionError::internal(&format!(
                    "Failed to parse result as json:\n{}",
                    json_str
                )))
            }
            Ok(result) => Ok(result),
        }
    }

    pub fn parse_expression(&self, expr: &str) -> Result<Expression> {
        let script = format!(
            r#"VegaUtils.parseExpression({})"#,
            serde_json::to_string(expr)?
        );

        let statement_result = self.nodejs_runtime.execute_statement(&script)?;
        let estree_expr: ESTreeExpression = Self::clean_and_parse_json(&statement_result)?;
        Ok(estree_expr.to_proto())
    }

    /// Function to evaluate a full Vega spec and return requested data and signal values
    pub fn eval_spec(&self, spec: &Value, watches: &Vec<Watch>) -> Result<Vec<WatchValue>> {
        let script = format!(
            r#"await VegaUtils.evalSpec({}, {})"#,
            serde_json::to_string(spec)?,
            serde_json::to_string(watches)?,
        );
        let statement_result = self.nodejs_runtime.execute_statement(&script)?;
        let watch_values: Vec<WatchValue> = Self::clean_and_parse_json(&statement_result)?;
        Ok(watch_values)
    }

    /// Evaluate a scalar signal expression in the presence of a collection of external signal
    /// values
    pub fn eval_scalar_expression(
        &self,
        expr: &str,
        scope: &HashMap<String, ScalarValue>,
    ) -> Result<ScalarValue> {
        // Add special signal for the requested expression
        let mut signals = vec![json!({"name": "_sig", "init": expr})];

        // Add scope signals
        for (sig, val) in scope {
            signals.push(json!({"name": sig.clone(), "value": val.to_json()?}))
        }

        // Create spec
        let spec = json!({ "signals": signals });

        // Create watch to request value of special signal
        let watches = vec![Watch {
            namespace: WatchNamespace::Signal,
            name: "_sig".to_string(),
            path: vec![],
        }];

        // Evaluate spec and extract signal value
        let watches = self.eval_spec(&spec, &watches)?;
        let scalar_value = ScalarValue::from_json(&watches[0].value)?;
        Ok(scalar_value)
    }

    pub fn eval_transform(
        &self,
        data_table: &VegaFusionTable,
        transforms: &[TransformSpec],
        config: &CompilationConfig,
    ) -> Result<(VegaFusionTable, Vec<ScalarValue>)> {
        // Initialize data vector with the input table and transforms
        let mut data = vec![json!({
            "name": "_dataset",
            "values": data_table.to_json(),
            "transform": transforms
        })];

        // Add additional dataset from compilation config
        for (name, val) in &config.data_scope {
            data.push(json!({"name": name.clone(), "values": val.to_json()}))
        }

        // Build signals vector from compilation config
        let mut signals: Vec<Value> = vec![];
        for (name, val) in &config.signal_scope {
            signals.push(json!({"name": name.clone(), "value": val.to_json()?}))
        }

        // Initialize watches with transformed dataset
        let mut watches = vec![Watch {
            namespace: WatchNamespace::Data,
            name: "_dataset".to_string(),
            path: vec![],
        }];

        // Add watches for signals produced by transforms
        for tx in transforms {
            for name in tx.output_signals() {
                watches.push(Watch {
                    namespace: WatchNamespace::Signal,
                    name,
                    path: vec![],
                })
            }
        }

        // Evaluate spec and extract signal value
        let spec = json!({
            "signals": signals,
            "data": data,
        });
        let watches = self.eval_spec(&spec, &watches)?;
        let dataset = VegaFusionTable::from_json(watches[0].value.clone(), 1024)?;

        let mut watch_signals = HashMap::new();
        for WatchValue { watch, value } in watches.iter().skip(1) {
            watch_signals.insert(watch.name.clone(), ScalarValue::from_json(value)?);
        }

        // Sort watch signal values by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = watch_signals.into_iter().sorted_by_key(
            |(k, v)| k.clone()
        ).unzip();

        Ok((dataset, signals_values))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchNamespace {
    Signal,
    Data,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Watch {
    pub namespace: WatchNamespace,
    pub name: String,
    pub path: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchValue {
    pub watch: Watch,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WatchValues {
    pub values: Vec<WatchValue>,
}

lazy_static! {
    static ref NODE_JS_RUNTIME: Arc<NodeJsRuntime> = Arc::new(NodeJsRuntime::try_new().unwrap());
}
pub fn vegajs_runtime() -> VegaJsRuntime {
    VegaJsRuntime::new((*NODE_JS_RUNTIME).clone())
}
