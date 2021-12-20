use crate::data::table::VegaFusionTableUtils;
use crate::expression::compiler::builtin_functions::datetime::{
    date_parsing::DATETIME_TO_MILLIS_LOCAL, datetime::DATETIME_COMPONENTS,
};
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::{
    cast_to, is_integer_datatype, is_string_datatype, ExprHelpers,
};
use crate::task_graph::task::TaskCall;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::options::CsvReadOptions;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::functions::BuiltinScalarFunction;
use datafusion::prelude::col;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use vegafusion_core::arrow::datatypes::TimeUnit;
use vegafusion_core::data::scalar::{ScalarValue, ScalarValueHelpers};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::tasks::data_url_task::Url;
use vegafusion_core::proto::gen::tasks::scan_url_format;
use vegafusion_core::proto::gen::tasks::{DataSourceTask, DataUrlTask, DataValuesTask};
use vegafusion_core::task_graph::task::{InputVariable, TaskDependencies};
use vegafusion_core::task_graph::task_value::TaskValue;
use crate::expression::compiler::builtin_functions::datetime::date_parsing::{DateParseMode, get_datetime_udf};

fn build_compilation_config(
    input_vars: &[InputVariable],
    values: &[TaskValue],
) -> CompilationConfig {
    // Build compilation config from input_vals
    let mut signal_scope: HashMap<String, ScalarValue> = HashMap::new();
    let mut data_scope: HashMap<String, VegaFusionTable> = HashMap::new();

    for (input_var, input_val) in input_vars.iter().zip(values) {
        match input_val {
            TaskValue::Scalar(value) => {
                signal_scope.insert(input_var.var.name.clone(), value.clone());
            }
            TaskValue::Table(table) => {
                data_scope.insert(input_var.var.name.clone(), table.clone());
            }
        }
    }

    // CompilationConfig is not Send, so use local scope here to make sure it's dropped
    // before the call to await below.
    CompilationConfig {
        signal_scope,
        data_scope,
        ..Default::default()
    }
}

#[async_trait]
impl TaskCall for DataUrlTask {
    async fn eval(&self, values: &[TaskValue]) -> Result<(TaskValue, Vec<TaskValue>)> {
        // Build compilation config for url signal (if any) and transforms (if any)
        let config = build_compilation_config(&self.input_vars(), values);

        // Build url string
        let url = match self.url.as_ref().unwrap() {
            Url::String(url) => url.clone(),
            Url::Expr(expr) => {
                let compiled = compile(expr, &config, None)?;
                let url_scalar = compiled.eval_to_scalar()?;
                url_scalar.to_scalar_string()?
            }
        };

        // Load data from URL
        let mut date_mode = DateParseMode::Local;
        let mut df = if url.ends_with(".csv") || url.ends_with(".tsv") {
            read_csv(url).await?
        } else if url.ends_with(".json") {
            // Vega uses the JavaScript Date.parse logic when parsing dates in json files.
            // Other formats (like CSV) use local date parsing.
            date_mode = DateParseMode::JavaScript;
            read_json(&url, self.batch_size as usize).await?
        } else if url.ends_with(".arrow") {
            date_mode = DateParseMode::JavaScript;
            read_arrow(&url).await?
        } else {
            return Err(VegaFusionError::internal(&format!(
                "Invalid url file extension {}",
                url
            )));
        };

        // Perform specialized parsing (right now, parse string columns named "date" to utc timestamps)
        if let Some(format_type) = &self.format_type {
            if let Some(parse) = &format_type.parse {
                if let scan_url_format::Parse::Object(formats) = &parse {
                    for spec in &formats.specs {
                        let datatype = &spec.datatype;
                        if datatype.starts_with("date") {
                            let schema = df.schema();
                            if let Ok(date_field) = schema.field_with_unqualified_name(&spec.name) {
                                let dtype = date_field.data_type();
                                let date_expr = if is_string_datatype(dtype) {
                                    let datetime_udf = get_datetime_udf(date_mode);
                                    let date_expr = Expr::ScalarUDF {
                                        fun: Arc::new(datetime_udf),
                                        args: vec![col(&spec.name)],
                                    };

                                    Expr::ScalarFunction {
                                        fun: BuiltinScalarFunction::ToTimestampMillis,
                                        args: vec![date_expr],
                                    }
                                } else if is_integer_datatype(dtype) {
                                    // Assume Year was parsed numerically
                                    Expr::ScalarUDF {
                                        fun: Arc::new(DATETIME_COMPONENTS.clone()),
                                        args: vec![col(&spec.name)],
                                    }
                                } else {
                                    continue;
                                };

                                let mut columns: Vec<_> = schema
                                    .fields()
                                    .iter()
                                    .filter_map(|field| {
                                        let name = field.name();
                                        if name == &spec.name {
                                            None
                                        } else {
                                            Some(col(name))
                                        }
                                    })
                                    .collect();
                                columns.push(date_expr.alias(&spec.name));
                                df = df.select(columns)?
                            }
                        }
                    }
                }
            }
        }

        // Standardize Timestamp columns to integer milliseconds in UTC
        let selection: Vec<_> = df.schema().fields().iter().map(|field| {
            if matches!(field.data_type(), DataType::Timestamp(_, _)) {
                Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::ToTimestampMillis,
                    args: vec![col(&field.name())],
                }.alias(field.name())
            } else {
                col(field.name())
            }
        }).collect();
        if !selection.is_empty() {
            df = df.select(selection)?;
        }

        print!("loaded schema: {:?}", df.schema());

        // Apply transforms (if any)
        let (transformed_df, output_values) = if self
            .pipeline
            .as_ref()
            .map(|p| !p.transforms.is_empty())
            .unwrap_or(false)
        {
            let pipeline = self.pipeline.as_ref().unwrap();
            pipeline.eval(df, &config).await?
        } else {
            // No transforms
            (df, Vec::new())
        };

        let table_value = TaskValue::Table(VegaFusionTable::from_dataframe(transformed_df).await?);

        Ok((table_value, output_values))
    }
}

#[async_trait]
impl TaskCall for DataValuesTask {
    async fn eval(&self, values: &[TaskValue]) -> Result<(TaskValue, Vec<TaskValue>)> {
        // Deserialize data into table
        let values_table = VegaFusionTable::from_ipc_bytes(&self.values)?;

        // Apply transforms (if any)
        let (transformed_table, output_values) = if self
            .pipeline
            .as_ref()
            .map(|p| !p.transforms.is_empty())
            .unwrap_or(false)
        {
            let pipeline = self.pipeline.as_ref().unwrap();
            let values_df = values_table.to_dataframe()?;
            let config = build_compilation_config(&self.input_vars(), values);
            let (df, output_values) = pipeline.eval(values_df, &config).await?;

            (VegaFusionTable::from_dataframe(df).await?, output_values)
        } else {
            // No transforms
            (values_table, Vec::new())
        };

        let table_value = TaskValue::Table(transformed_table);

        Ok((table_value, output_values))
    }
}

#[async_trait]
impl TaskCall for DataSourceTask {
    async fn eval(&self, values: &[TaskValue]) -> Result<(TaskValue, Vec<TaskValue>)> {
        let mut config = build_compilation_config(&self.input_vars(), values);

        // Remove source table from config
        let source_table = config.data_scope.remove(&self.source).unwrap();

        // Apply transforms (if any)
        let (transformed_table, output_values) = if self
            .pipeline
            .as_ref()
            .map(|p| !p.transforms.is_empty())
            .unwrap_or(false)
        {
            let pipeline = self.pipeline.as_ref().unwrap();
            let values_df = source_table.to_dataframe()?;
            let (df, output_values) = pipeline.eval(values_df, &config).await?;
            (VegaFusionTable::from_dataframe(df).await?, output_values)
        } else {
            // No transforms
            (source_table, Vec::new())
        };

        let table_value = TaskValue::Table(transformed_table);
        Ok((table_value, output_values))
    }
}

async fn read_csv(url: String) -> Result<Arc<dyn DataFrame>> {
    // Build options
    let csv_opts = if url.ends_with(".tsv") {
        CsvReadOptions::new()
            .delimiter(b'\t')
            .file_extension(".tsv")
    } else {
        CsvReadOptions::new()
    };

    let mut ctx = ExecutionContext::new();

    if url.starts_with("http://") || url.starts_with("https://") {
        // Perform get request to collect file contents as text
        let body = reqwest::get(url.clone())
            .await
            .external(&format!("Failed to get URL data from {}", url))?
            .text()
            .await
            .external("Failed to convert URL data to text")?;

        // Write contents to temp csv file
        let tempdir = tempfile::TempDir::new().unwrap();
        let filename = format!("file.{}", csv_opts.file_extension);
        let filepath = tempdir.path().join(filename).to_str().unwrap().to_string();
        let mut file = File::create(filepath.clone()).unwrap();
        writeln!(file, "{}", body).unwrap();

        // Load through VegaFusionTable so that temp file can be deleted
        let df = ctx
            .read_csv(tempdir.path().to_str().unwrap(), csv_opts)
            .await
            .unwrap();
        let table = VegaFusionTable::from_dataframe(df).await.unwrap();
        let df = table.to_dataframe().unwrap();
        Ok(df)
    } else {
        Ok(ctx.read_csv(url, csv_opts).await?)
    }
}

async fn read_json(url: &str, batch_size: usize) -> Result<Arc<dyn DataFrame>> {
    // Read to json Value from local file or url.
    let value: serde_json::Value = if url.starts_with("http://") || url.starts_with("https://") {
        // Perform get request to collect file contents as text
        let body = reqwest::get(url)
            .await
            .external(&format!("Failed to get URL data from {}", url))?
            .text()
            .await
            .external("Failed to convert URL data to text")?;

        serde_json::from_str(&body)?
    } else {
        // Assume local file
        let mut file = tokio::fs::File::open(url)
            .await
            .external(&format!("Failed to open as local file: {}", url))?;

        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .await
            .external("Failed to read file contents to string")?;

        serde_json::from_str(&json_str)?
    };

    VegaFusionTable::from_json(&value, batch_size)?.to_dataframe()
}

async fn read_arrow(url: &str) -> Result<Arc<dyn DataFrame>> {
    // Read to json Value from local file or url.
    let buffer = if url.starts_with("http://") || url.starts_with("https://") {
        // Perform get request to collect file contents as text
        reqwest::get(url)
            .await
            .external(&format!("Failed to get URL data from {}", url))?
            .bytes()
            .await
            .external("Failed to convert URL data to text")?
    } else {
        // Assume local file
        let mut file = tokio::fs::File::open(url)
            .await
            .external(&format!("Failed to open as local file: {}", url))?;

        let mut buffer: Vec<u8> = Vec::new();
        file.read_to_end(&mut buffer)
            .await
            .external("Failed to read file contents")?;

        bytes::Bytes::from(buffer)
    };

    let reader = std::io::Cursor::new(buffer);

    // Try parsing file as both File and IPC formats
    let (schema, batches) = if let Ok(arrow_reader) = FileReader::try_new(reader.clone()) {
        let schema = arrow_reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for v in arrow_reader {
            batches.push(v.with_context(|| "Failed to read arrow batch".to_string())?);
        }
        (schema, batches)
    } else if let Ok(arrow_reader) = StreamReader::try_new(reader.clone()) {
        let schema = arrow_reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for v in arrow_reader {
            batches.push(v.with_context(|| "Failed to read arrow batch".to_string())?);
        }
        (schema, batches)
    } else {
        let f = FileReader::try_new(reader).unwrap();
        println!("f: {:?}", f.schema());
        return Err(VegaFusionError::parse(format!(
            "Failed to read arrow file at {}",
            url
        )));
    };

    VegaFusionTable::try_new(schema, batches)?.to_dataframe()
}
