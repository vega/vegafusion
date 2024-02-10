use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::ExprHelpers;
use crate::task_graph::task::TaskCall;

use async_trait::async_trait;

use datafusion_expr::{expr, lit, Expr};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::data::dataset::VegaFusionDataset;
use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::pipeline::{remove_order_col, TransformPipelineUtils};

use vegafusion_common::data::scalar::{ScalarValue, ScalarValueHelpers};
use vegafusion_common::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};

use vegafusion_core::proto::gen::tasks::data_url_task::Url;
use vegafusion_core::proto::gen::tasks::scan_url_format;
use vegafusion_core::proto::gen::tasks::scan_url_format::Parse;
use vegafusion_core::proto::gen::tasks::{DataSourceTask, DataUrlTask, DataValuesTask};
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::task_graph::task::{InputVariable, TaskDependencies};
use vegafusion_core::task_graph::task_value::TaskValue;

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema};
use vegafusion_common::column::flat_col;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::{is_integer_datatype, is_string_datatype};
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use vegafusion_core::spec::visitors::extract_inline_dataset;
use vegafusion_dataframe::connection::Connection;
use vegafusion_dataframe::csv::CsvReadOptions;
use vegafusion_dataframe::dataframe::DataFrame;
use vegafusion_datafusion_udfs::udfs::datetime::date_to_utc_timestamp::DATE_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::make_utc_timestamp::MAKE_UTC_TIMESTAMP;
use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::to_utc_timestamp::TO_UTC_TIMESTAMP_UDF;

pub fn build_compilation_config(
    input_vars: &[InputVariable],
    values: &[TaskValue],
    tz_config: &Option<RuntimeTzConfig>,
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
        tz_config: *tz_config,
        ..Default::default()
    }
}

#[async_trait]
impl TaskCall for DataUrlTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        inline_datasets: HashMap<String, VegaFusionDataset>,
        conn: Arc<dyn Connection>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        // Build compilation config for url signal (if any) and transforms (if any)
        let config = build_compilation_config(&self.input_vars(), values, tz_config);

        // Build url string
        let url = match self.url.as_ref().unwrap() {
            Url::String(url) => url.clone(),
            Url::Expr(expr) => {
                let compiled = compile(expr, &config, None)?;
                let url_scalar = compiled.eval_to_scalar()?;
                url_scalar.to_scalar_string()?
            }
        };

        // Strip trailing Hash, e.g. https://foo.csv#1234 -> https://foo.csv
        let url_parts: Vec<&str> = url.splitn(2, '#').collect();
        let url = url_parts.first().cloned().unwrap_or(&url).to_string();

        // Handle references to vega default datasets (e.g. "data/us-10m.json")
        let url = check_builtin_dataset(url);

        // Load data from URL
        let parse = self.format_type.as_ref().and_then(|fmt| fmt.parse.clone());
        let file_type = self.format_type.as_ref().and_then(|fmt| fmt.r#type.clone());

        // Vega-Lite sets unspecified file types to "json", so we don't want this to take
        // precedence over file extension
        let file_type = if file_type == Some("json".to_string()) {
            None
        } else {
            file_type.as_deref()
        };

        let registered_tables = conn.tables().await?;
        let df = if let Some(inline_name) = extract_inline_dataset(&url) {
            let inline_name = inline_name.trim().to_string();
            if let Some(inline_dataset) = inline_datasets.get(&inline_name) {
                match inline_dataset {
                    VegaFusionDataset::Table { table, .. } => {
                        conn.scan_arrow(table.clone().with_ordering()?).await?
                    }
                    VegaFusionDataset::DataFrame(df) => df.clone(),
                }
            } else if registered_tables.contains_key(&inline_name) {
                conn.scan_table(&inline_name).await?
            } else {
                return Err(VegaFusionError::internal(format!(
                    "No inline dataset named {inline_name}"
                )));
            }
        } else if file_type == Some("csv") || (file_type.is_none() && url.ends_with(".csv")) {
            read_csv(&url, &parse, conn, false).await?
        } else if file_type == Some("tsv") || (file_type.is_none() && url.ends_with(".tsv")) {
            read_csv(&url, &parse, conn, true).await?
        } else if file_type == Some("json") || (file_type.is_none() && url.ends_with(".json")) {
            read_json(&url, conn).await?
        } else if file_type == Some("arrow")
            || (file_type.is_none() && (url.ends_with(".arrow") || url.ends_with(".feather")))
        {
            read_arrow(&url, conn).await?
        } else if file_type == Some("parquet")
            || (file_type.is_none() && (url.ends_with(".parquet")))
        {
            read_parquet(&url, conn).await?
        } else {
            return Err(VegaFusionError::internal(format!(
                "Invalid url file extension {url}"
            )));
        };

        // Ensure there is an ordering column present
        let df = if df.schema().column_with_name(ORDER_COL).is_none() {
            df.with_index(ORDER_COL).await?
        } else {
            df
        };

        // Perform any up-front type conversions
        let df = pre_process_column_types(df).await?;

        // Process datetime columns
        let df = process_datetimes(&parse, df, &config.tz_config).await?;

        eval_sql_df(df, &self.pipeline, &config).await
    }
}

async fn eval_sql_df(
    sql_df: Arc<dyn DataFrame>,
    pipeline: &Option<TransformPipeline>,
    config: &CompilationConfig,
) -> Result<(TaskValue, Vec<TaskValue>)> {
    // Apply transforms (if any)
    let (transformed_df, output_values) = if pipeline
        .as_ref()
        .map(|p| !p.transforms.is_empty())
        .unwrap_or(false)
    {
        let pipeline = pipeline.as_ref().unwrap();
        pipeline.eval_sql(sql_df, config).await?
    } else {
        // No transforms, just remove any ordering column
        let sql_df = remove_order_col(sql_df).await?;
        (sql_df.collect().await?, Vec::new())
    };

    let table_value = TaskValue::Table(transformed_df);

    Ok((table_value, output_values))
}

lazy_static! {
    static ref BUILT_IN_DATASETS: HashSet<&'static str> = vec![
        "7zip.png",
        "airports.csv",
        "annual-precip.json",
        "anscombe.json",
        "barley.json",
        "birdstrikes.csv",
        "budget.json",
        "budgets.json",
        "burtin.json",
        "cars.json",
        "co2-concentration.csv",
        "countries.json",
        "crimea.json",
        "disasters.csv",
        "driving.json",
        "earthquakes.json",
        "ffox.png",
        "flare-dependencies.json",
        "flare.json",
        "flights-10k.json",
        "flights-200k.arrow",
        "flights-200k.json",
        "flights-20k.json",
        "flights-2k.json",
        "flights-3m.csv",
        "flights-5k.json",
        "flights-airport.csv",
        "football.json",
        "gapminder-health-income.csv",
        "gapminder.json",
        "gimp.png",
        "github.csv",
        "income.json",
        "iowa-electricity.csv",
        "jobs.json",
        "la-riots.csv",
        "londonBoroughs.json",
        "londonCentroids.json",
        "londonTubeLines.json",
        "lookup_groups.csv",
        "lookup_people.csv",
        "miserables.json",
        "monarchs.json",
        "movies.json",
        "normal-2d.json",
        "obesity.json",
        "ohlc.json",
        "penguins.json",
        "platformer-terrain.json",
        "points.json",
        "political-contributions.json",
        "population_engineers_hurricanes.csv",
        "population.json",
        "seattle-weather.csv",
        "seattle-weather-hourly-normals.csv",
        "sp500-2000.csv",
        "sp500.csv",
        "stocks.csv",
        "udistrict.json",
        "unemployment-across-industries.json",
        "unemployment.tsv",
        "uniform-2d.json",
        "us-10m.json",
        "us-employment.csv",
        "us-state-capitals.json",
        "volcano.json",
        "weather.csv",
        "weather.json",
        "wheat.json",
        "windvectors.csv",
        "world-110m.json",
        "zipcodes.csv",
    ]
    .into_iter()
    .collect();
}

const DATASET_BASE: &str = "https://raw.githubusercontent.com/vega/vega-datasets";
const DATASET_TAG: &str = "v2.3.0";

fn check_builtin_dataset(url: String) -> String {
    if let Some(dataset) = url.strip_prefix("data/") {
        let path = std::path::Path::new(&url);
        if !path.exists() && BUILT_IN_DATASETS.contains(dataset) {
            format!("{DATASET_BASE}/{DATASET_TAG}/data/{dataset}")
        } else {
            url
        }
    } else {
        url
    }
}

async fn pre_process_column_types(df: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>> {
    let mut selections: Vec<Expr> = Vec::new();
    let mut pre_proc_needed = false;
    for field in df.schema().fields.iter() {
        if field.data_type() == &DataType::LargeUtf8 {
            // Work around https://github.com/apache/arrow-rs/issues/2654 by converting
            // LargeUtf8 to Utf8
            selections.push(
                Expr::Cast(expr::Cast {
                    expr: Box::new(flat_col(field.name())),
                    data_type: DataType::Utf8,
                })
                .alias(field.name()),
            );
            pre_proc_needed = true;
        } else {
            selections.push(flat_col(field.name()))
        }
    }
    if pre_proc_needed {
        df.select(selections).await
    } else {
        Ok(df)
    }
}

async fn process_datetimes(
    parse: &Option<Parse>,
    sql_df: Arc<dyn DataFrame>,
    tz_config: &Option<RuntimeTzConfig>,
) -> Result<Arc<dyn DataFrame>> {
    // Perform specialized date parsing
    let mut date_fields: Vec<String> = Vec::new();
    let mut df = sql_df;
    if let Some(scan_url_format::Parse::Object(formats)) = parse {
        for spec in &formats.specs {
            let datatype = &spec.datatype;
            if datatype.starts_with("date") || datatype.starts_with("utc") {
                let schema = df.schema_df()?;
                if let Ok(date_field) = schema.field_with_unqualified_name(&spec.name) {
                    let dtype = date_field.data_type();
                    let date_expr = if is_string_datatype(dtype) {
                        let default_input_tz_str = tz_config
                            .map(|tz_config| tz_config.default_input_tz.to_string())
                            .unwrap_or_else(|| "UTC".to_string());

                        Expr::ScalarUDF(expr::ScalarUDF {
                            fun: Arc::new((*STR_TO_UTC_TIMESTAMP_UDF).clone()),
                            args: vec![flat_col(&spec.name), lit(default_input_tz_str)],
                        })
                    } else if is_integer_datatype(dtype) {
                        // Assume Year was parsed numerically, use local time
                        let tz_config =
                            tz_config.with_context(|| "No local timezone info provided")?;
                        Expr::ScalarUDF(expr::ScalarUDF {
                            fun: Arc::new((*MAKE_UTC_TIMESTAMP).clone()),
                            args: vec![
                                flat_col(&spec.name),                        // year
                                lit(0),                                      // month
                                lit(1),                                      // day
                                lit(0),                                      // hour
                                lit(0),                                      // minute
                                lit(0),                                      // second
                                lit(0),                                      // millisecond
                                lit(tz_config.default_input_tz.to_string()), // time zone
                            ],
                        })
                    } else {
                        continue;
                    };

                    // Add to date_fields if special date processing was performed
                    date_fields.push(date_field.name().clone());

                    let mut columns: Vec<_> = schema
                        .fields()
                        .iter()
                        .filter_map(|field| {
                            let name = field.name();
                            if name == &spec.name {
                                None
                            } else {
                                Some(flat_col(name))
                            }
                        })
                        .collect();
                    columns.push(date_expr.alias(&spec.name));
                    df = df.select(columns).await?
                }
            }
        }
    }

    // Standardize other Timestamp columns (those that weren't created above) to integer
    // milliseconds
    let schema = df.schema();
    let selection: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if !date_fields.contains(field.name()) {
                let expr = match field.data_type() {
                    DataType::Timestamp(_, tz) => match tz {
                        Some(tz) => {
                            // Timestamp has explicit timezone
                            Expr::ScalarUDF(expr::ScalarUDF {
                                fun: Arc::new((*TO_UTC_TIMESTAMP_UDF).clone()),
                                args: vec![flat_col(field.name()), lit(tz.as_ref())],
                            })
                        }
                        _ => {
                            // Naive timestamp, interpret as default_input_tz
                            let tz_config =
                                tz_config.with_context(|| "No local timezone info provided")?;

                            Expr::ScalarUDF(expr::ScalarUDF {
                                fun: Arc::new((*TO_UTC_TIMESTAMP_UDF).clone()),
                                args: vec![
                                    flat_col(field.name()),
                                    lit(tz_config.default_input_tz.to_string()),
                                ],
                            })
                        }
                    },
                    DataType::Date64 => {
                        let tz_config =
                            tz_config.with_context(|| "No local timezone info provided")?;

                        Expr::ScalarUDF(expr::ScalarUDF {
                            fun: Arc::new((*TO_UTC_TIMESTAMP_UDF).clone()),
                            args: vec![
                                flat_col(field.name()),
                                lit(tz_config.default_input_tz.to_string()),
                            ],
                        })
                    }
                    DataType::Date32 => {
                        let tz_config =
                            tz_config.with_context(|| "No local timezone info provided")?;

                        Expr::ScalarUDF(expr::ScalarUDF {
                            fun: Arc::new((*DATE_TO_UTC_TIMESTAMP_UDF).clone()),
                            args: vec![flat_col(field.name()), lit(tz_config.local_tz.to_string())],
                        })
                    }
                    _ => flat_col(field.name()),
                };

                Ok(if matches!(expr, Expr::Alias(_)) {
                    expr
                } else {
                    expr.alias(field.name())
                })
            } else {
                Ok(flat_col(field.name()))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    df.select(selection).await
}

#[async_trait]
impl TaskCall for DataValuesTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        conn: Arc<dyn Connection>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        // Deserialize data into table
        let values_table = VegaFusionTable::from_ipc_bytes(&self.values)?;
        if values_table.schema.fields.is_empty() {
            return Ok((TaskValue::Table(values_table), Default::default()));
        }

        // Return early for empty input data unless first transform is a sequence
        // (which generates its own data)
        if values_table.num_rows() == 0 {
            if let Some(pipeline) = &self.pipeline {
                if let Some(first_tx) = pipeline.transforms.get(0) {
                    if !matches!(first_tx.transform_kind(), TransformKind::Sequence(_)) {
                        return Ok((TaskValue::Table(values_table), Default::default()));
                    }
                }
            }
        }

        // Add ordering column
        let values_table = values_table.with_ordering()?;

        // Get parse format for date processing
        let parse = self.format_type.as_ref().and_then(|fmt| fmt.parse.clone());

        // Apply transforms (if any)
        let (transformed_table, output_values) = if self
            .pipeline
            .as_ref()
            .map(|p| !p.transforms.is_empty())
            .unwrap_or(false)
        {
            let pipeline = self.pipeline.as_ref().unwrap();

            let config = build_compilation_config(&self.input_vars(), values, tz_config);

            // Process datetime columns
            let df = conn.scan_arrow(values_table).await?;
            let sql_df = process_datetimes(&parse, df, &config.tz_config).await?;

            let (table, output_values) = pipeline.eval_sql(sql_df, &config).await?;

            (table, output_values)
        } else {
            // No transforms
            let values_df = conn.scan_arrow(values_table).await?;
            let values_df = process_datetimes(&parse, values_df, tz_config).await?;
            (values_df.collect().await?, Vec::new())
        };

        let table_value = TaskValue::Table(transformed_table);

        Ok((table_value, output_values))
    }
}

#[async_trait]
impl TaskCall for DataSourceTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        conn: Arc<dyn Connection>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        let input_vars = self.input_vars();
        let mut config = build_compilation_config(&input_vars, values, tz_config);

        // Remove source table from config
        let source_table = config.data_scope.remove(&self.source).with_context(|| {
            format!(
                "Missing source {} for task with input variables\n{:#?}",
                self.source, input_vars
            )
        })?;

        // Add ordering column
        let source_table = source_table.with_ordering()?;

        // Apply transforms (if any)
        let (transformed_table, output_values) = if self
            .pipeline
            .as_ref()
            .map(|p| !p.transforms.is_empty())
            .unwrap_or(false)
        {
            let pipeline = self.pipeline.as_ref().unwrap();
            let sql_df = conn.scan_arrow(source_table).await?;
            let (table, output_values) = pipeline.eval_sql(sql_df, &config).await?;

            (table, output_values)
        } else {
            // No transforms
            (source_table, Vec::new())
        };

        let table_value = TaskValue::Table(transformed_table);
        Ok((table_value, output_values))
    }
}

async fn read_csv(
    url: &str,
    parse: &Option<Parse>,
    conn: Arc<dyn Connection>,
    is_tsv: bool,
) -> Result<Arc<dyn DataFrame>> {
    // Build base CSV options
    let mut csv_opts = if is_tsv {
        CsvReadOptions {
            delimiter: b'\t',
            ..Default::default()
        }
    } else {
        Default::default()
    };

    // Add file extension based on URL
    if let Some(ext) = Path::new(url).extension().and_then(|ext| ext.to_str()) {
        csv_opts.file_extension = ext.to_string();
    } else {
        csv_opts.file_extension = "".to_string();
    }

    // Build schema from Vega parse options
    let schema = build_csv_schema(parse);
    csv_opts.schema = schema;

    conn.scan_csv(url, csv_opts).await
}

fn build_csv_schema(parse: &Option<Parse>) -> Option<Schema> {
    // Get HashMap of provided columns formats
    let format_specs = if let Some(parse) = parse {
        match parse {
            Parse::String(_) => {
                // auto, use inferred schema
                return None;
            }
            Parse::Object(field_specs) => field_specs
                .specs
                .iter()
                .map(|spec| (spec.name.clone(), spec.datatype.clone()))
                .collect(),
        }
    } else {
        HashMap::new()
    };

    let new_fields: Vec<_> = format_specs
        .iter()
        .map(|(name, vega_type)| {
            let dtype = match vega_type.as_str() {
                "number" => DataType::Float64,
                "boolean" => DataType::Boolean,
                "date" => DataType::Utf8, // Parse as string, convert to date later
                "string" => DataType::Utf8,
                _ => DataType::Utf8,
            };
            Field::new(name, dtype, true)
        })
        .collect();

    Some(Schema::new(new_fields))
}

async fn read_json(url: &str, conn: Arc<dyn Connection>) -> Result<Arc<dyn DataFrame>> {
    // Read to json Value from local file or url.
    let value: serde_json::Value = if url.starts_with("http://") || url.starts_with("https://") {
        // Perform get request to collect file contents as text
        let body = make_request_client()
            .get(url)
            .send()
            .await
            .external(&format!("Failed to get URL data from {url}"))?
            .text()
            .await
            .external("Failed to convert URL data to text")?;

        serde_json::from_str(&body)?
    } else if let Some(bucket_path) = url.strip_prefix("s3://") {
        let s3= AmazonS3Builder::from_env().with_url(url).build().with_context(||
            "Failed to initialize s3 connection from environment variables.\n\
                See https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env".to_string()
        )?;
        let Some((_, path)) = bucket_path.split_once('/') else {
            return Err(VegaFusionError::specification(format!(
                "Invalid s3 URL: {url}"
            )));
        };
        let path = object_store::path::Path::from_url_path(path)?;
        let get_result = s3.get(&path).await?;
        let b = get_result.bytes().await?;
        let text = String::from_utf8_lossy(b.as_ref());
        serde_json::from_str(text.as_ref())?
    } else {
        // Assume local file
        let mut file = tokio::fs::File::open(url)
            .await
            .external(format!("Failed to open as local file: {url}"))?;

        let mut json_str = String::new();
        file.read_to_string(&mut json_str)
            .await
            .external("Failed to read file contents to string")?;

        serde_json::from_str(&json_str)?
    };

    let table = VegaFusionTable::from_json(&value)?.with_ordering()?;

    conn.scan_arrow(table).await
}

async fn read_arrow(url: &str, conn: Arc<dyn Connection>) -> Result<Arc<dyn DataFrame>> {
    conn.scan_arrow_file(url).await
}

async fn read_parquet(url: &str, conn: Arc<dyn Connection>) -> Result<Arc<dyn DataFrame>> {
    conn.scan_parquet(url).await
}

pub fn make_request_client() -> ClientWithMiddleware {
    // Retry up to 3 times with increasing intervals between attempts.
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}
