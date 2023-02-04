use crate::connection::SqlConnection;
use crate::dataframe::SqlDataFrame;
use crate::dialect::Dialect;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions as DfCsvReadOptions, SessionContext};
use log::Level;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datatypes::cast_to;
use vegafusion_common::error::{Result, ResultWithContext, ToExternalError};
use vegafusion_dataframe::connection::Connection;
use vegafusion_dataframe::csv::CsvReadOptions;
use vegafusion_dataframe::dataframe::DataFrame;

#[derive(Clone)]
pub struct DataFusionConnection {
    dialect: Arc<Dialect>,
    ctx: Arc<SessionContext>,
}

impl DataFusionConnection {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self {
            dialect: Arc::new(make_datafusion_dialect()),
            ctx,
        }
    }
}

pub fn make_datafusion_dialect() -> Dialect {
    Dialect::datafusion()
}

#[async_trait::async_trait]
impl Connection for DataFusionConnection {
    fn id(&self) -> String {
        "datafusion".to_string()
    }

    async fn tables(&self) -> Result<HashMap<String, Schema>> {
        let catalog_names = self.ctx.catalog_names();
        let first_catalog_name = catalog_names.get(0).unwrap();
        let catalog = self.ctx.catalog(first_catalog_name).unwrap();

        let schema_provider_names = catalog.schema_names();
        let first_schema_provider_name = schema_provider_names.get(0).unwrap();
        let schema_provider = catalog.schema(first_schema_provider_name).unwrap();

        let mut tables: HashMap<String, Schema> = HashMap::new();
        for table_name in schema_provider.table_names() {
            let schema = schema_provider.table(&table_name).await.unwrap().schema();
            tables.insert(table_name, schema.as_ref().clone());
        }
        Ok(tables)
    }

    async fn scan_table(&self, name: &str) -> Result<Arc<dyn DataFrame>> {
        Ok(Arc::new(
            SqlDataFrame::try_new(Arc::new(self.clone()), name).await?,
        ))
    }

    async fn scan_arrow(&self, table: VegaFusionTable) -> Result<Arc<dyn DataFrame>> {
        // Get batch schema
        let batch_schema = if table.batches.is_empty() {
            None
        } else {
            Some(table.batches.get(0).unwrap().schema())
        };

        // Create memtable
        let mem_table = MemTable::try_new(
            batch_schema.clone().unwrap_or_else(|| table.schema.clone()),
            vec![table.batches.clone()],
        )
        .with_context(|| {
            format!(
                "memtable failure with schema {:#?} and batch schema {:#?}",
                table.schema, batch_schema
            )
        })?;

        // Create a fresh context because we don't want to override tables in self.ctx
        // let ctx = make_session_context();
        let ctx = SessionContext::new();

        // Register memtable with context
        ctx.register_table("tbl", Arc::new(mem_table))?;
        let sql_conn = DataFusionConnection::new(Arc::new(ctx));
        Ok(Arc::new(
            SqlDataFrame::try_new(Arc::new(sql_conn), "tbl").await?,
        ))
    }

    async fn scan_csv(&self, url: &str, opts: CsvReadOptions) -> Result<Arc<dyn DataFrame>> {
        // Build DataFusion's CsvReadOptions
        let mut df_csv_opts = DfCsvReadOptions {
            has_header: opts.has_header,
            delimiter: opts.delimiter,
            file_extension: opts.file_extension.as_str(),
            ..DfCsvReadOptions::default()
        };
        df_csv_opts.schema = opts.schema.as_ref();

        if url.starts_with("http://") || url.starts_with("https://") {
            // Perform get request to collect file contents as text
            let body = make_request_client()
                .get(url)
                .send()
                .await
                .external(&format!("Failed to get URL data from {url}"))?
                .text()
                .await
                .external("Failed to convert URL data to text")?;

            // Write contents to temp csv file
            let tempdir = tempfile::TempDir::new().unwrap();
            let filename = format!("file.{}", df_csv_opts.file_extension);
            let filepath = tempdir.path().join(filename).to_str().unwrap().to_string();

            {
                let mut file = File::create(filepath.clone()).unwrap();
                writeln!(file, "{body}").unwrap();
            }

            let path = tempdir.path().to_str().unwrap();

            // Build final csv schema that combines the requested and inferred schemas
            let final_schema = build_csv_schema(&df_csv_opts, path).await?;
            df_csv_opts = df_csv_opts.schema(&final_schema);

            // Load through VegaFusionTable so that temp file can be deleted
            let df = self.ctx.read_csv(path, df_csv_opts).await?;

            let schema: SchemaRef = Arc::new(df.schema().into()) as SchemaRef;
            let batches = df.collect().await?;
            let table = VegaFusionTable::try_new(schema, batches)?;

            let table = table.with_ordering()?;
            self.scan_arrow(table).await
        } else {
            // Build final csv schema that combines the requested and inferred schemas
            let final_schema = build_csv_schema(&df_csv_opts, url).await?;
            df_csv_opts = df_csv_opts.schema(&final_schema);

            let df = self.ctx.read_csv(url, df_csv_opts).await.unwrap();
            let schema: SchemaRef = Arc::new(df.schema().into()) as SchemaRef;
            let batches = df.collect().await?;
            let table = VegaFusionTable::try_new(schema, batches)?;
            let table = table.with_ordering()?;
            self.scan_arrow(table).await
        }
    }
}

#[async_trait::async_trait]
impl SqlConnection for DataFusionConnection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable> {
        info!("{}", query);
        let df = self.ctx.sql(query).await?;

        let result_fields: Vec<_> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.field().clone().with_nullable(true))
            .collect();
        let expected_fields: Vec<_> = schema
            .fields
            .iter()
            .map(|f| f.clone().with_nullable(true))
            .collect();
        let df = if result_fields == expected_fields {
            df
        } else {
            // Coerce dataframe columns to match expected schema
            let selections = expected_fields
                .iter()
                .map(|f| {
                    Ok(cast_to(flat_col(f.name()), f.data_type(), df.schema())?.alias(f.name()))
                })
                .collect::<Result<Vec<_>>>()?;
            df.select(selections)?
        };

        let schema: SchemaRef = Arc::new(df.schema().into()) as SchemaRef;
        let batches = df.collect().await?;
        let res = VegaFusionTable::try_new(schema, batches)?;

        if log_enabled!(Level::Debug) {
            debug!("\n{}", res.pretty_format(Some(5)).unwrap());
            debug!("{:?}", res.schema);
        }

        Ok(res)
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    fn to_connection(&self) -> Arc<dyn Connection> {
        Arc::new(self.clone())
    }
}

/// Build final schema by combining the input and inferred schemas
async fn build_csv_schema(
    csv_opts: &DfCsvReadOptions<'_>,
    uri: impl Into<String>,
) -> Result<Schema> {
    let ctx = SessionContext::new();
    let table_path = ListingTableUrl::parse(uri.into().as_str())?;
    let target_partitions = ctx.copied_config().target_partitions();
    let listing_options = csv_opts.to_listing_options(target_partitions);
    let inferred_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await?;

    // Get HashMap of provided columns formats
    let field_types: HashMap<_, _> = if let Some(schema) = csv_opts.schema {
        schema
            .fields
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    } else {
        // No input schema provided, use inferred schema
        return Ok(inferred_schema.as_ref().clone());
    };

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

pub fn make_request_client() -> ClientWithMiddleware {
    // Retry up to 3 times with increasing intervals between attempts.
    let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
    ClientBuilder::new(reqwest::Client::new())
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}
