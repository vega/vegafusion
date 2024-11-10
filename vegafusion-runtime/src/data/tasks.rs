use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::ExprHelpers;
use crate::task_graph::task::TaskCall;
use std::borrow::Cow;

use async_trait::async_trait;

use datafusion_expr::{expr, lit, Expr};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use vegafusion_core::data::dataset::VegaFusionDataset;

use crate::task_graph::timezone::RuntimeTzConfig;
use crate::transform::pipeline::TransformPipelineUtils;
use cfg_if::cfg_if;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::options::{ArrowReadOptions, ReadOptions};
use datafusion::prelude::{CsvReadOptions, DataFrame, SessionContext};
use datafusion_common::config::TableOptions;
use datafusion_functions::expr_fn::make_date;
use std::sync::Arc;

use vegafusion_common::data::scalar::{ScalarValue, ScalarValueHelpers};
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};

use vegafusion_core::proto::gen::tasks::data_url_task::Url;
use vegafusion_core::proto::gen::tasks::scan_url_format;
use vegafusion_core::proto::gen::tasks::scan_url_format::Parse;
use vegafusion_core::proto::gen::tasks::{DataSourceTask, DataUrlTask, DataValuesTask};
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::task_graph::task::{InputVariable, TaskDependencies};
use vegafusion_core::task_graph::task_value::TaskValue;

use crate::data::util::{DataFrameUtils, SessionContextUtils};
use crate::transform::utils::str_to_timestamp;

use object_store::ObjectStore;
use vegafusion_common::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use vegafusion_common::column::flat_col;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::{is_integer_datatype, is_string_datatype};
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use vegafusion_core::spec::visitors::extract_inline_dataset;

#[cfg(feature = "s3")]
use object_store::aws::AmazonS3Builder;

#[cfg(feature = "http")]
use object_store::{http::HttpBuilder, ClientOptions};

#[cfg(feature = "fs")]
use tokio::io::AsyncReadExt;

#[cfg(feature = "parquet")]
use {datafusion::prelude::ParquetReadOptions, vegafusion_common::error::ToExternalError};

#[cfg(target_arch = "wasm32")]
use object_store_wasm::HttpStore;

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
        ctx: Arc<SessionContext>,
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

        let df = if let Some(inline_name) = extract_inline_dataset(&url) {
            let inline_name = inline_name.trim().to_string();
            if let Some(inline_dataset) = inline_datasets.get(&inline_name) {
                match inline_dataset {
                    VegaFusionDataset::Table { table, .. } => {
                        let table = table.clone().with_ordering()?;
                        ctx.vegafusion_table(table).await?
                    }
                    VegaFusionDataset::Plan { plan } => {
                        ctx.execute_logical_plan(plan.clone()).await?
                    }
                }
            } else if let Ok(df) = ctx.table(&inline_name).await {
                df
            } else {
                return Err(VegaFusionError::internal(format!(
                    "No inline dataset named {inline_name}"
                )));
            }
        } else if file_type == Some("csv") || (file_type.is_none() && url.ends_with(".csv")) {
            read_csv(&url, &parse, ctx, false).await?
        } else if file_type == Some("tsv") || (file_type.is_none() && url.ends_with(".tsv")) {
            read_csv(&url, &parse, ctx, true).await?
        } else if file_type == Some("json") || (file_type.is_none() && url.ends_with(".json")) {
            read_json(&url, ctx).await?
        } else if file_type == Some("arrow")
            || (file_type.is_none() && (url.ends_with(".arrow") || url.ends_with(".feather")))
        {
            read_arrow(&url, ctx).await?
        } else if file_type == Some("parquet")
            || (file_type.is_none() && (url.ends_with(".parquet")))
        {
            cfg_if! {
                if #[cfg(any(feature = "parquet"))] {
                    read_parquet(&url, ctx).await?
                } else {
                    return Err(VegaFusionError::internal(format!(
                        "Enable parquet support by enabling the `parquet` feature flag"
                    )))
                }
            }
        } else {
            return Err(VegaFusionError::internal(format!(
                "Invalid url file extension {url}"
            )));
        };

        // Ensure there is an ordering column present
        let df = if df.schema().inner().column_with_name(ORDER_COL).is_none() {
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
    sql_df: DataFrame,
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
        (
            sql_df.collect_to_table().await?.without_ordering()?,
            Vec::new(),
        )
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

async fn pre_process_column_types(df: DataFrame) -> Result<DataFrame> {
    let mut selections: Vec<Expr> = Vec::new();
    let mut pre_proc_needed = false;
    for field in df.schema().fields().iter() {
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
        Ok(df.select(selections)?)
    } else {
        Ok(df)
    }
}

/// After processing, all datetime columns are converted to Timestamptz and Date32
async fn process_datetimes(
    parse: &Option<Parse>,
    sql_df: DataFrame,
    tz_config: &Option<RuntimeTzConfig>,
) -> Result<DataFrame> {
    // Perform specialized date parsing
    let mut date_fields: Vec<String> = Vec::new();
    let mut df = sql_df;
    if let Some(scan_url_format::Parse::Object(formats)) = parse {
        for spec in &formats.specs {
            let datatype = &spec.datatype;
            if datatype.starts_with("date") || datatype.starts_with("utc") {
                // look for format string
                let (typ, fmt) = if let Some((typ, fmt)) = datatype.split_once(':') {
                    if fmt.starts_with("'") && fmt.ends_with("'") {
                        (typ.to_lowercase(), Some(fmt[1..fmt.len() - 1].to_string()))
                    } else {
                        (typ.to_lowercase(), None)
                    }
                } else {
                    (datatype.to_lowercase(), None)
                };

                let schema = df.schema();
                if let Ok(date_field) = schema.field_with_unqualified_name(&spec.name) {
                    let dtype = date_field.data_type();
                    let date_expr = if is_string_datatype(dtype) {
                        // Compute default timezone
                        let default_input_tz_str = if typ == "utc" || tz_config.is_none() {
                            "UTC".to_string()
                        } else {
                            tz_config.unwrap().default_input_tz.to_string()
                        };

                        if let Some(fmt) = fmt {
                            // Parse with single explicit format
                            str_to_timestamp(
                                flat_col(&spec.name),
                                &default_input_tz_str,
                                schema,
                                Some(fmt.as_str()),
                            )?
                        } else {
                            // Parse with auto formats, then localize to default_input_tz
                            str_to_timestamp(
                                flat_col(&spec.name),
                                &default_input_tz_str,
                                schema,
                                None,
                            )?
                        }
                    } else if is_integer_datatype(dtype) {
                        // Assume Year was parsed numerically, return Date32
                        make_date(flat_col(&spec.name), lit(1), lit(1))
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
            if !date_fields.contains(field.name()) {
                let expr = match field.data_type() {
                    DataType::Timestamp(_, tz) => match tz {
                        Some(_) => {
                            // Timestamp has explicit timezone, all good
                            flat_col(field.name())
                        }
                        _ => {
                            // Naive timestamp, localize to default_input_tz
                            let tz_config =
                                tz_config.with_context(|| "No local timezone info provided")?;

                            flat_col(field.name()).try_cast_to(
                                &DataType::Timestamp(
                                    TimeUnit::Millisecond,
                                    Some(tz_config.default_input_tz.to_string().into()),
                                ),
                                schema,
                            )?
                        }
                    },
                    DataType::Date64 => {
                        let tz_config =
                            tz_config.with_context(|| "No local timezone info provided")?;

                        // Cast to naive timestamp, then localize to timestamp with timezone
                        flat_col(field.name())
                            .try_cast_to(&DataType::Timestamp(TimeUnit::Millisecond, None), schema)?
                            .try_cast_to(
                                &DataType::Timestamp(
                                    TimeUnit::Millisecond,
                                    Some(tz_config.default_input_tz.to_string().into()),
                                ),
                                schema,
                            )?
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

    Ok(df.select(selection)?)
}

#[async_trait]
impl TaskCall for DataValuesTask {
    async fn eval(
        &self,
        values: &[TaskValue],
        tz_config: &Option<RuntimeTzConfig>,
        _inline_datasets: HashMap<String, VegaFusionDataset>,
        ctx: Arc<SessionContext>,
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
                if let Some(first_tx) = pipeline.transforms.first() {
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
            let df = ctx.vegafusion_table(values_table).await?;
            let sql_df = process_datetimes(&parse, df, &config.tz_config).await?;

            let (table, output_values) = pipeline.eval_sql(sql_df, &config).await?;

            (table, output_values)
        } else {
            // No transforms
            let values_df = ctx.vegafusion_table(values_table).await?;
            let values_df = process_datetimes(&parse, values_df, tz_config).await?;
            (values_df.collect_to_table().await?, Vec::new())
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
        ctx: Arc<SessionContext>,
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
            let sql_df = ctx.vegafusion_table(source_table).await?;
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
    ctx: Arc<SessionContext>,
    is_tsv: bool,
) -> Result<DataFrame> {
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
    let ext = if let Some(ext) = Path::new(url).extension().and_then(|ext| ext.to_str()) {
        ext.to_string()
    } else {
        "".to_string()
    };

    csv_opts.file_extension = ext.as_str();

    maybe_register_object_stores_for_url(&ctx, url)?;

    // Build schema from Vega parse options
    let schema = build_csv_schema(&csv_opts, parse, url, &ctx).await?;
    csv_opts.schema = Some(&schema);

    Ok(ctx.read_csv(url, csv_opts).await?)
}

/// Build final schema by combining the input and inferred schemas
async fn build_csv_schema(
    csv_opts: &CsvReadOptions<'_>,
    parse: &Option<Parse>,
    uri: impl Into<String>,
    ctx: &SessionContext,
) -> Result<Schema> {
    // Get HashMap of provided columns formats
    let format_specs = if let Some(parse) = parse {
        match parse {
            Parse::String(_) => {
                // auto, use inferred schema
                HashMap::new()
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

    // Map formats to fields
    let field_types: HashMap<_, _> = format_specs
        .iter()
        .map(|(name, vega_type)| {
            let dtype = match vega_type.as_str() {
                "number" => DataType::Float64,
                "boolean" => DataType::Boolean,
                "date" => DataType::Utf8, // Parse as string, convert to date later
                "string" => DataType::Utf8,
                _ => DataType::Utf8,
            };
            (name.clone(), dtype)
        })
        .collect();

    // Get inferred schema
    let table_path = ListingTableUrl::parse(uri.into().as_str())?;
    let listing_options =
        csv_opts.to_listing_options(&ctx.copied_config(), TableOptions::default());
    let inferred_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await?;

    // Override inferred schema based on parse options
    let new_fields: Vec<_> = inferred_schema
        .fields()
        .iter()
        .map(|field| {
            // Use provided field type, but fall back to string for unprovided columns
            let dtype = field_types
                .get(field.name())
                .cloned()
                .unwrap_or(DataType::Utf8);
            Field::new(field.name(), dtype, true)
        })
        .collect();
    Ok(Schema::new(new_fields))
}

async fn read_json(url: &str, ctx: Arc<SessionContext>) -> Result<DataFrame> {
    let value: serde_json::Value =
        if let Some(base_url) = maybe_register_object_stores_for_url(&ctx, url)? {
            // Create single use object store that points directly to file
            let store = ctx.runtime_env().object_store(&base_url)?;
            let child_url = url.strip_prefix(&base_url.to_string()).unwrap();
            match store.get(&child_url.into()).await {
                Ok(get_res) => {
                    let bytes = get_res.bytes().await?.to_vec();
                    let text: Cow<str> = String::from_utf8_lossy(&bytes);
                    serde_json::from_str(text.as_ref())?
                }
                Err(e) => {
                    cfg_if::cfg_if! {
                        if #[cfg(feature="http")] {
                            if url.starts_with("http://") || url.starts_with("https://") {
                                // Fallback to direct reqwest implementation. This is needed in some cases because
                                // the object-store http implementation has stricter requirements on what the
                                // server provides. For example the content-length header is required.
                                let client = reqwest::Client::new();
                                let response = client
                                    .get(url)
                                    .send()
                                    .await
                                    .external(format!("Failed to fetch URL: {url}"))?;

                                let text = response
                                    .text()
                                    .await
                                    .external("Failed to read response as text")?;
                                serde_json::from_str(&text)?
                            } else {
                                return Err(VegaFusionError::from(e));
                            }
                        } else {
                            return Err(VegaFusionError::from(e));
                        }
                    }
                }
            }
        } else {
            cfg_if::cfg_if! {
                if #[cfg(feature="fs")] {
                    // Assume local file
                    let mut file = tokio::fs::File::open(url)
                        .await
                        .external(format!("Failed to open as local file: {url}"))?;

                    let mut json_str = String::new();
                    file.read_to_string(&mut json_str)
                        .await
                        .external("Failed to read file contents to string")?;

                    serde_json::from_str(&json_str)?
                } else {
                    return Err(VegaFusionError::internal(
                        "The `fs` feature flag must be enabled for file system support"
                    ));
                }
            }
        };

    let table = VegaFusionTable::from_json(&value)?.with_ordering()?;
    ctx.vegafusion_table(table).await
}

async fn read_arrow(url: &str, ctx: Arc<SessionContext>) -> Result<DataFrame> {
    maybe_register_object_stores_for_url(&ctx, url)?;
    Ok(ctx.read_arrow(url, ArrowReadOptions::default()).await?)
}

#[cfg(feature = "parquet")]
async fn read_parquet(url: &str, ctx: Arc<SessionContext>) -> Result<DataFrame> {
    maybe_register_object_stores_for_url(&ctx, url)?;
    Ok(ctx.read_parquet(url, ParquetReadOptions::default()).await?)
}

fn maybe_register_object_stores_for_url(
    ctx: &SessionContext,
    url: &str,
) -> Result<Option<ObjectStoreUrl>> {
    // Handle object store registration for non-local sources
    #[cfg(any(feature = "http", feature = "http-wasm"))]
    {
        let maybe_register_http_store = |prefix: &str| -> Result<Option<ObjectStoreUrl>> {
            if let Some(path) = url.strip_prefix(prefix) {
                let Some((root, _)) = path.split_once('/') else {
                    return Err(VegaFusionError::specification(format!(
                        "Invalid https URL: {url}"
                    )));
                };
                let base_url_str = format!("https://{root}");
                let base_url = url::Url::parse(&base_url_str)?;

                // Register store for url if not already registered
                let object_store_url = ObjectStoreUrl::parse(&base_url_str)?;
                if ctx
                    .runtime_env()
                    .object_store(object_store_url.clone())
                    .is_err()
                {
                    cfg_if! {
                        if #[cfg(feature="http")] {
                            let client_options = ClientOptions::new().with_allow_http(true);
                            let http_store = HttpBuilder::new()
                                .with_url(base_url.clone())
                                .with_client_options(client_options)
                                .build()?;
                            ctx.register_object_store(&base_url, Arc::new(http_store));
                        } else {
                            let http_store = HttpStore::new(base_url.clone());
                            ctx.register_object_store(&base_url, Arc::new(http_store));
                        }
                    }
                }
                return Ok(Some(object_store_url));
            }
            Ok(None)
        };

        // Register https://
        if let Some(url) = maybe_register_http_store("https://")? {
            return Ok(Some(url));
        }

        // Register http://
        if let Some(url) = maybe_register_http_store("http://")? {
            return Ok(Some(url));
        }
    }

    // Register s3://
    #[cfg(feature = "s3")]
    if let Some(bucket_path) = url.strip_prefix("s3://") {
        let Some((bucket, _)) = bucket_path.split_once('/') else {
            return Err(VegaFusionError::specification(format!(
                "Invalid s3 URL: {url}"
            )));
        };
        // Register store for url if not already registered
        let base_url_str = format!("s3://{bucket}/");
        let object_store_url = ObjectStoreUrl::parse(&base_url_str)?;
        if ctx
            .runtime_env()
            .object_store(object_store_url.clone())
            .is_err()
        {
            let base_url = url::Url::parse(&base_url_str)?;
            let s3 = AmazonS3Builder::from_env().with_url(base_url.clone()).build().with_context(||
            "Failed to initialize s3 connection from environment variables.\n\
                See https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env".to_string()
            )?;
            ctx.register_object_store(&base_url, Arc::new(s3));
        }
        return Ok(Some(object_store_url));
    }

    Ok(None)
}
