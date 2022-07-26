/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::data::table::VegaFusionTableUtils;
use crate::expression::compiler::builtin_functions::date_time::date_parsing::{
    get_datetime_udf, DateParseMode,
};
use crate::expression::compiler::builtin_functions::date_time::datetime::make_datetime_components_udf;
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::{
    cast_to, is_integer_datatype, is_string_datatype, ExprHelpers,
};
use crate::task_graph::task::TaskCall;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::options::CsvReadOptions;
use datafusion::logical_plan::Expr;
use datafusion::prelude::{col, SessionContext};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use vegafusion_core::arrow::datatypes::TimeUnit;
use vegafusion_core::data::dataset::VegaFusionDataset;

use crate::data::dataset::VegaFusionDatasetUtils;
use crate::expression::compiler::builtin_functions::date_time::local_to_utc::make_to_utc_millis_fn;
use crate::task_graph::timezone::RuntimeTzConfig;
use vegafusion_core::data::scalar::{ScalarValue, ScalarValueHelpers};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::tasks::data_url_task::Url;
use vegafusion_core::proto::gen::tasks::scan_url_format;
use vegafusion_core::proto::gen::tasks::scan_url_format::Parse;
use vegafusion_core::proto::gen::tasks::{DataSourceTask, DataUrlTask, DataValuesTask};
use vegafusion_core::task_graph::task::{InputVariable, TaskDependencies};
use vegafusion_core::task_graph::task_value::TaskValue;

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

        let date_mode = DateParseMode::JavaScript;
        let df = if let Some(inline_name) = url.strip_prefix("vegafusion+dataset://") {
            let inline_name = inline_name.trim().to_string();
            if let Some(inline_dataset) = inline_datasets.get(&inline_name) {
                inline_dataset.to_dataframe()?
            } else {
                return Err(VegaFusionError::internal(format!(
                    "No inline dataset named {}",
                    inline_name
                )));
            }
        } else if url.ends_with(".csv") || url.ends_with(".tsv") {
            read_csv(url, &parse).await?
        } else if url.ends_with(".json") {
            read_json(&url, self.batch_size as usize).await?
        } else if url.ends_with(".arrow") || url.ends_with(".feather") {
            read_arrow(&url).await?
        } else {
            return Err(VegaFusionError::internal(&format!(
                "Invalid url file extension {}",
                url
            )));
        };

        let df = process_datetimes(&parse, date_mode, df, tz_config)?;

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

const DATASET_CDN_BASE: &str = "https://cdn.jsdelivr.net/npm/vega-datasets";
const DATASET_TAG: &str = "v2.2.0";

fn check_builtin_dataset(url: String) -> String {
    if let Some(dataset) = url.strip_prefix("data/") {
        let path = std::path::Path::new(&url);
        if !path.exists() && BUILT_IN_DATASETS.contains(dataset) {
            format!("{}@{}/data/{}", DATASET_CDN_BASE, DATASET_TAG, dataset)
        } else {
            url
        }
    } else {
        url
    }
}

fn process_datetimes(
    parse: &Option<Parse>,
    date_mode: DateParseMode,
    df: Arc<DataFrame>,
    tz_config: &Option<RuntimeTzConfig>,
) -> Result<Arc<DataFrame>> {
    // Perform specialized date parsing
    let mut date_fields: Vec<String> = Vec::new();
    let mut df = df;
    if let Some(scan_url_format::Parse::Object(formats)) = parse {
        for spec in &formats.specs {
            let datatype = &spec.datatype;
            if datatype.starts_with("date") || datatype.starts_with("utc") {
                let date_mode = if datatype.starts_with("utc") {
                    DateParseMode::Utc
                } else {
                    date_mode
                };

                let schema = df.schema();
                if let Ok(date_field) = schema.field_with_unqualified_name(&spec.name) {
                    date_fields.push(date_field.name().clone());
                    let dtype = date_field.data_type();
                    let date_expr = if is_string_datatype(dtype) {
                        let datetime_udf = get_datetime_udf(date_mode, tz_config);

                        Expr::ScalarUDF {
                            fun: Arc::new(datetime_udf),
                            args: vec![col(&spec.name)],
                        }
                    } else if is_integer_datatype(dtype) {
                        // Assume Year was parsed numerically, use local time
                        let tz_config =
                            tz_config.with_context(|| "No local timezone info provided")?;
                        Expr::ScalarUDF {
                            fun: Arc::new(make_datetime_components_udf(tz_config.default_input_tz)),
                            args: vec![col(&spec.name)],
                        }
                    } else if let DataType::Timestamp(_, tz) = dtype {
                        match tz {
                            Some(tz) if tz.to_lowercase() == "utc" => {
                                let timestamp_millis = cast_to(
                                    col(&spec.name),
                                    &DataType::Timestamp(
                                        TimeUnit::Millisecond,
                                        Some("UTC".to_string()),
                                    ),
                                    schema,
                                )?;
                                cast_to(timestamp_millis, &DataType::Int64, schema).unwrap()
                            }
                            _ => {
                                let timestamp_millis = cast_to(
                                    col(&spec.name),
                                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                                    schema,
                                )?;
                                // Treat as local
                                let tz_config =
                                    tz_config.with_context(|| "No local timezone info provided")?;
                                Expr::ScalarUDF {
                                    fun: Arc::new(make_to_utc_millis_fn(&tz_config)),
                                    args: vec![timestamp_millis],
                                }
                            }
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

    // Standardize other Timestamp columns (those that weren't created above) to integer
    // milliseconds
    let schema = df.schema();
    let selection: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if !date_fields.contains(field.name())
                && matches!(field.data_type(), DataType::Timestamp(_, _))
            {
                let expr = match field.data_type() {
                    DataType::Timestamp(_, tz) => match tz {
                        Some(tz) if tz.to_lowercase() == "utc" => {
                            let timestamp_millis = cast_to(
                                col(field.name()),
                                &DataType::Timestamp(
                                    TimeUnit::Millisecond,
                                    Some("UTC".to_string()),
                                ),
                                schema,
                            )?;
                            cast_to(timestamp_millis, &DataType::Int64, schema).unwrap()
                        }
                        _ => {
                            let timestamp_millis = cast_to(
                                col(field.name()),
                                &DataType::Timestamp(TimeUnit::Millisecond, None),
                                schema,
                            )?;
                            let tz_config =
                                tz_config.with_context(|| "No local timezone info provided")?;
                            Expr::ScalarUDF {
                                fun: Arc::new(make_to_utc_millis_fn(&tz_config)),
                                args: vec![timestamp_millis],
                            }
                        }
                    },
                    _ => unreachable!(),
                };
                Ok(expr.alias(field.name()))
            } else {
                Ok(col(field.name()))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(df.select(selection)?)
}

#[async_trait]
impl TaskCall for DataValuesTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        // Deserialize data into table
        let values_table = VegaFusionTable::from_ipc_bytes(&self.values)?;

        // Get parse format for date processing
        let parse = self.format_type.as_ref().and_then(|fmt| fmt.parse.clone());
        let date_mode = DateParseMode::JavaScript;

        // Apply transforms (if any)
        let (transformed_table, output_values) = if self
            .pipeline
            .as_ref()
            .map(|p| !p.transforms.is_empty())
            .unwrap_or(false)
        {
            let pipeline = self.pipeline.as_ref().unwrap();
            let values_df = values_table.to_dataframe()?;
            let values_df = process_datetimes(&parse, date_mode, values_df, tz_config)?;

            let config = build_compilation_config(&self.input_vars(), values, tz_config);
            let (df, output_values) = pipeline.eval(values_df, &config).await?;

            (VegaFusionTable::from_dataframe(df).await?, output_values)
        } else {
            // No transforms
            let values_df = values_table.to_dataframe()?;
            let values_df = process_datetimes(&parse, date_mode, values_df, tz_config)?;
            (
                VegaFusionTable::from_dataframe(values_df).await?,
                Vec::new(),
            )
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
    ) -> Result<(TaskValue, Vec<TaskValue>)> {
        let input_vars = self.input_vars();
        let mut config = build_compilation_config(&input_vars, values, tz_config);

        // Remove source table from config
        let source_table = config.data_scope.remove(&self.source).unwrap_or_else(|| {
            panic!(
                "Missing source {} for task with input variables\n{:#?}",
                self.source, input_vars
            )
        });

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

async fn read_csv(url: String, parse: &Option<Parse>) -> Result<Arc<DataFrame>> {
    // Build base CSV options
    let csv_opts = if url.ends_with(".tsv") {
        CsvReadOptions::new()
            .delimiter(b'\t')
            .file_extension(".tsv")
    } else {
        CsvReadOptions::new()
    };

    let ctx = SessionContext::new();

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

        {
            let mut file = File::create(filepath.clone()).unwrap();
            writeln!(file, "{}", body).unwrap();
        }

        let path = tempdir.path().to_str().unwrap();
        let schema = build_csv_schema(&csv_opts, path, parse).await?;
        let csv_opts = csv_opts.schema(&schema);

        // Load through VegaFusionTable so that temp file can be deleted
        let df = ctx.read_csv(path, csv_opts).await.unwrap();
        let table = VegaFusionTable::from_dataframe(df).await.unwrap();
        let df = table.to_dataframe().unwrap();
        Ok(df)
    } else {
        let schema = build_csv_schema(&csv_opts, &url, parse).await?;
        let csv_opts = csv_opts.schema(&schema);
        Ok(ctx.read_csv(url, csv_opts).await?)
    }
}

async fn build_csv_schema(
    csv_opts: &CsvReadOptions<'_>,
    uri: impl Into<String>,
    parse: &Option<Parse>,
) -> Result<SchemaRef> {
    let ctx = SessionContext::new();
    let table_path = ListingTableUrl::parse(uri.into().as_str())?;
    let target_partitions = ctx.copied_config().target_partitions;
    let listing_options = csv_opts.to_listing_options(target_partitions);
    let inferred_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await?;

    // Get HashMap of provided columns formats
    let format_specs = if let Some(parse) = parse {
        match parse {
            Parse::String(_) => {
                // auto, return inferred schema as-is
                return Ok(inferred_schema);
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

    // Override inferred schema based on parse options
    let new_fields: Vec<_> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            let dtype = if let Some(f) = format_specs.get(field.name()) {
                match f.as_str() {
                    "number" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    "date" => DataType::Utf8, // Parse as string, convert to date later
                    "string" => DataType::Utf8,
                    _ => DataType::Utf8,
                }
            } else {
                // Unspecified, use String
                DataType::Utf8
            };
            Field::new(field.name(), dtype, true)
        })
        .collect();
    Ok(SchemaRef::new(Schema::new(new_fields)))
}

async fn read_json(url: &str, batch_size: usize) -> Result<Arc<DataFrame>> {
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

async fn read_arrow(url: &str) -> Result<Arc<DataFrame>> {
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
    let (schema, batches) = if let Ok(arrow_reader) = FileReader::try_new(reader.clone(), None) {
        let schema = arrow_reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for v in arrow_reader {
            batches.push(v.with_context(|| "Failed to read arrow batch".to_string())?);
        }
        (schema, batches)
    } else if let Ok(arrow_reader) = StreamReader::try_new(reader.clone(), None) {
        let schema = arrow_reader.schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        for v in arrow_reader {
            batches.push(v.with_context(|| "Failed to read arrow batch".to_string())?);
        }
        (schema, batches)
    } else {
        let _f = FileReader::try_new(reader, None).unwrap();
        return Err(VegaFusionError::parse(format!(
            "Failed to read arrow file at {}",
            url
        )));
    };

    VegaFusionTable::try_new(schema, batches)?.to_dataframe()
}
