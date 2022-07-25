/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::scalar::ScalarValue;
use dssim::{Dssim, DssimImage};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

use std::io::{Read, Write};
use std::ops::Deref;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::{fs, thread};
// use vega_fusion::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};
// use vega_fusion::expression::compiler::config::CompilationConfig;
// use vega_fusion::expression::compiler::utils::ScalarValueHelpers;
// use vega_fusion::spec::transform::TransformSpec;
use self::super::estree_expression::ESTreeExpression;
use itertools::Itertools;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::data::table::VegaFusionTable;

use vegafusion_core::planning::watch::{ExportUpdateBatch, Watch, WatchNamespace, WatchValue};
use vegafusion_core::proto::gen::expression::Expression;

use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::spec::transform::TransformSpec;

use vegafusion_rt_datafusion::expression::compiler::config::CompilationConfig;

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

        let mut proc = Command::new("node")
            .args(&["-i", "--experimental-repl-await"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(working_dir)
            .spawn()
            .external("Failed to launch nodejs")?;

        let out = Self::child_stream_to_vec(
            proc.stdout
                .take()
                .with_context(|| "Failed to create stdout stream".to_string())?,
        );

        let this = Self {
            proc: Mutex::new(proc),
            out,
        };

        // Wait for node repl to start up
        std::thread::sleep(std::time::Duration::from_millis(1000));

        // Set abort_on_empty to true because some versions of the node repl display a welcome
        // message, and some do not.
        let welcome_message = this.read_output();
        println!("Initialized node: {}\n", welcome_message);

        this.execute_statement(
            "util = require('util'); util.inspect.replDefaults.maxStringLength = Infinity;",
        )
        .unwrap();

        let str_result = this
            .execute_statement("VegaUtils = require('./vegajsRuntime.js')")
            .unwrap();
        println!("VegaUtils require output: {}", str_result);

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
            .external("Failed to acquire lock to nodejs process")?;
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

        Ok(self.read_output())
    }

    fn read_output(&self) -> String {
        let boundary = "\n> ".as_bytes();
        let bytes_read = loop {
            let mut vec = self.out.deref().lock().unwrap();
            let n = vec.len() as i32;
            if n >= 3 && &vec[(n - 3) as usize..] == boundary {
                // The output ends with newline then prompt
                let cloned = vec.clone();
                vec.clear();
                break cloned;
            } else if n == 2 && vec[..] == boundary[1..] {
                // The output is only a prompt
                let mut cloned = vec.clone();
                // Add leading newly so logic that follows doesn't need a special case
                cloned.insert(0, b'\n');
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
        s.trim().to_string()
    }

    pub fn local_timezone(&self) -> Result<String> {
        self.execute_statement("Intl.DateTimeFormat().resolvedOptions().timeZone")
            .map(|tz| tz.trim_matches('\'').to_string())
    }
}

#[derive(Clone)]
pub struct VegaJsRuntime {
    pub nodejs_runtime: Arc<NodeJsRuntime>,
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
            Err(_err) => Err(VegaFusionError::internal(&format!(
                "Failed to parse result as json:\n{}",
                json_str
            ))),
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
    pub fn eval_spec(&self, spec: &Value, watches: &[Watch]) -> Result<Vec<WatchValue>> {
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
        config: &CompilationConfig,
    ) -> Result<ScalarValue> {
        // Add special signal for the requested expression
        let mut signals = vec![json!({"name": "_sig", "init": expr})];

        // Add scope signals
        for (sig, val) in &config.signal_scope {
            signals.push(json!({"name": sig.clone(), "value": val.to_json()?}))
        }

        // Add datasets (for use in data expressions)
        let mut data = vec![];
        for (name, val) in &config.data_scope {
            data.push(json!({"name": name, "values": val.to_json()}))
        }

        // Create spec
        let spec = json!({ "signals": signals, "data": data });

        // Create watch to request value of special signal
        let watches = vec![Watch {
            namespace: WatchNamespace::Signal,
            name: "_sig".to_string(),
            scope: vec![],
        }];

        // Evaluate spec and extract signal value
        let watches = self.eval_spec(&spec, watches.as_slice())?;
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
        // Add additional dataset from compilation config
        let mut data = Vec::new();
        for (name, val) in &config.data_scope {
            data.push(json!({"name": name.clone(), "values": val.to_json()}))
        }

        data.push(json!({
            "name": "_dataset",
            "values": data_table.to_json(),
            "transform": transforms
        }));

        // Build signals vector from compilation config
        let mut signals: Vec<Value> = vec![];
        for (name, val) in &config.signal_scope {
            signals.push(json!({"name": name.clone(), "value": val.to_json()?}))
        }

        // Initialize watches with transformed dataset
        let mut watches = vec![Watch {
            namespace: WatchNamespace::Data,
            name: "_dataset".to_string(),
            scope: vec![],
        }];

        // Add watches for signals produced by transforms
        for tx in transforms {
            for name in tx.output_signals() {
                watches.push(Watch {
                    namespace: WatchNamespace::Signal,
                    name,
                    scope: vec![],
                })
            }
        }

        // Evaluate spec and extract signal value
        let spec = json!({
            "signals": signals,
            "data": data,
        });

        // println!("{}", serde_json::to_string_pretty(&spec).unwrap());

        let watches = self.eval_spec(&spec, &watches)?;
        let dataset = VegaFusionTable::from_json(&watches[0].value, 1024)?;

        let mut watch_signals = HashMap::new();
        for WatchValue { watch, value } in watches.iter().skip(1) {
            watch_signals.insert(watch.name.clone(), ScalarValue::from_json(value)?);
        }

        // Sort watch signal values by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = watch_signals
            .into_iter()
            .sorted_by_key(|(k, _v)| k.clone())
            .unzip();

        Ok((dataset, signals_values))
    }

    pub fn export_spec_single(
        &self,
        spec: &ChartSpec,
        format: ExportImageFormat,
    ) -> Result<ExportImage> {
        // Write input spec out to a temp file
        let spec_tmpfile = tempfile::NamedTempFile::new().unwrap();
        let spec_tmppath = spec_tmpfile.path().to_str().unwrap();
        let spec_str = serde_json::to_string(spec).unwrap();
        fs::write(spec_tmppath, spec_str).expect("Failed to write temp file");

        // Create temporary file for result
        let result_tmpfile = tempfile::NamedTempFile::new().unwrap();
        let result_tmppath = result_tmpfile.path().to_str().unwrap();

        let _res_out = self
            .nodejs_runtime
            .execute_statement(&format!(
                "\
    spec = fs.readFileSync('{spec_tmppath}', {{encoding: 'utf8'}});\
    await VegaUtils.exportSingle(JSON.parse(spec), '{result_tmppath}', {format})\
    ",
                spec_tmppath = unquote_path(spec_tmppath),
                result_tmppath = unquote_path(result_tmppath),
                format = serde_json::to_string(&format).unwrap(),
            ))
            .expect("export single failed");

        let result_str = fs::read_to_string(result_tmppath)
            .with_context(|| format!("Failed to read {}", result_tmppath))?;

        let result_img: ExportImage = serde_json::from_str(&result_str).unwrap();
        Ok(result_img)
    }

    pub fn export_spec_sequence(
        &self,
        spec: &ChartSpec,
        format: ExportImageFormat,
        init: ExportUpdateBatch,
        updates: Vec<ExportUpdateBatch>,
        watches: Vec<Watch>,
    ) -> Result<Vec<(ExportImage, Vec<WatchValue>)>> {
        // Write input spec out to a temp file
        let spec_tmpfile = tempfile::NamedTempFile::new().unwrap();
        let spec_tmppath = spec_tmpfile.path().to_str().unwrap();
        let spec_str = serde_json::to_string(spec).unwrap();
        fs::write(spec_tmppath, spec_str).expect("Failed to write temp file");

        // Create temporary file for result
        let result_tmpfile = tempfile::NamedTempFile::new().unwrap();
        let result_tmppath = result_tmpfile.path().to_str().unwrap();

        let init_json_str = serde_json::to_string(&init).unwrap();
        let updates_json_str = serde_json::to_string(&updates).unwrap();
        let watches_json_str = serde_json::to_string(&watches).unwrap();

        let _res_out = self.nodejs_runtime.execute_statement(&format!("\
    spec = fs.readFileSync('{spec_tmppath}', {{encoding: 'utf8'}});\
    await VegaUtils.exportSequence(JSON.parse(spec), '{result_tmppath}', {format}, {init}, {updates}, {watches})",
                                                         spec_tmppath=unquote_path(spec_tmppath),
                                                         result_tmppath=unquote_path(result_tmppath),
                                                         init=init_json_str,
                                                         updates=updates_json_str,
                                                         watches=watches_json_str,
                                                         format=serde_json::to_string(&format).unwrap(),
        )).unwrap();

        if _res_out != "undefined" {
            println!("nodejs command output: {}", _res_out);
        }

        let result_str = fs::read_to_string(result_tmppath)
            .with_context(|| format!("Failed to read {}", result_tmppath))?;

        let result_img: Vec<(ExportImage, Vec<WatchValue>)> =
            serde_json::from_str(&result_str).unwrap();
        Ok(result_img)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportImageFormat {
    Png,
    Svg,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExportImage {
    Png(String),
    Svg(String),
}

impl ExportImage {
    pub fn save(&self, path: &str, add_ext: bool) -> Result<String> {
        let mut path = path.to_string();
        match self {
            ExportImage::Svg(svg) => if !add_ext || path.ends_with(".svg") {
                fs::write(&path, svg)
            } else {
                path.push_str(".svg");
                fs::write(&path, svg)
            }
            .with_context(|| format!("Failed to write svg image to {}", path))?,
            ExportImage::Png(png_b64) => {
                let png_bytes = base64::decode(png_b64)
                    .external("Failed to decdode base64 encoded png image")?;
                if !add_ext || path.ends_with(".png") {
                    fs::write(&path, png_bytes)
                } else {
                    path.push_str(".png");
                    fs::write(&path, png_bytes)
                }
                .with_context(|| format!("Failed to write png image to {}", path))?
            }
        };

        Ok(path)
    }

    pub fn to_dssim(&self, attr: &Dssim) -> Result<DssimImage<f32>> {
        if !matches!(self, ExportImage::Png(_)) {
            return Err(VegaFusionError::internal("Only PNG image supported"));
        }
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let tmppath = tmpfile.path().to_str().unwrap();
        self.save(tmppath, false)?;

        let img = dssim::load_image(attr, tmppath)
            .external("Failed to create DSSIM image for comparison")?;
        Ok(img)
    }

    pub fn compare(&self, other: &Self) -> Result<(f64, Option<Vec<u8>>)> {
        let mut attr = Dssim::new();
        attr.set_save_ssim_maps(1);
        let this_img = self.to_dssim(&attr)?;
        let other_img = other.to_dssim(&attr)?;
        let (diff, ssim_maps) = attr.compare(&this_img, &other_img);
        // println!("ssim_map: {:?}", ssim_map);

        if diff > 0.0 {
            let map_meta = ssim_maps[0].clone();
            let avgssim = map_meta.ssim as f32;

            let out: Vec<_> = map_meta
                .map
                .pixels()
                .map(|ssim| {
                    let max = 1_f32 - ssim;
                    let maxsq = max * max;
                    rgb::RGBA8 {
                        r: to_byte(maxsq * 16.0),
                        g: to_byte(max * 3.0),
                        b: to_byte(max / ((1_f32 - avgssim) * 4_f32)),
                        a: 255,
                    }
                })
                .collect();
            let png_res =
                lodepng::encode32(&out, map_meta.map.width(), map_meta.map.height()).unwrap();
            Ok((diff.into(), Some(png_res)))
        } else {
            Ok((diff.into(), None))
        }
    }
}

fn to_byte(i: f32) -> u8 {
    if i <= 0.0 {
        0
    } else if i >= 255.0 / 256.0 {
        255
    } else {
        (i * 256.0) as u8
    }
}

lazy_static! {
    static ref NODE_JS_RUNTIME: Arc<NodeJsRuntime> = Arc::new(NodeJsRuntime::try_new().unwrap());
}
pub fn vegajs_runtime() -> VegaJsRuntime {
    VegaJsRuntime::new((*NODE_JS_RUNTIME).clone())
}

fn unquote_path(path: &str) -> String {
    path.replace('\\', r#"\\"#)
}
