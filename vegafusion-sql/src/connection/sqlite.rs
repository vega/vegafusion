use std::collections::HashMap;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use vegafusion_core::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use vegafusion_core::arrow::datatypes::{DataType, Schema, SchemaRef};
use vegafusion_core::arrow::record_batch::RecordBatch;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, VegaFusionError};
use async_trait::async_trait;
use sqlgen::ast::Query;
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlx::sqlite::SqliteRow;
use crate::connection::SqlDatabaseConnection;


#[derive(Clone, Debug)]
pub struct SqLiteConnection {
    pub pool: Arc<SqlitePool>,
    pub table: String,
    pub schema: Schema,
}

impl SqLiteConnection {
    // Also input a table name (regular or temporary) to use as the source
    pub fn new(pool: Arc<SqlitePool>, table: &str, schema: &Schema) -> Self {
        Self {
            pool,
            table: table.to_string(),
            schema: schema.clone()
        }
    }
}

#[async_trait]
impl SqlDatabaseConnection for SqLiteConnection {
    async fn fetch_query(&self, query: &Query, schema: &Schema) -> Result<VegaFusionTable> {

        // Convert query AST to string
        let mut dialect: Dialect = Default::default();
        dialect.quote_style = Some('"');

        let query = query.sql(&dialect).map_err(
            |err| VegaFusionError::internal(err.to_string())
        )?;

        println!("Query str:\n{}", query);

        // Should fetch batches of partition size instead of fetching all
        let recs = sqlx::query(&query)
            .fetch_all(self.pool.as_ref())
            .await
            .unwrap_or_else(|_| panic!("Failed to fetch result for query: {}", query));

        // iterate over columns according to schema
        // Loop over columns
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (field_index, field) in schema.fields().iter().enumerate() {
            let array = match field.data_type() {
                DataType::Int64 => {
                    let values = extract_row_values::<i64>(&recs, field_index);
                    Arc::new(Int64Array::from(values)) as ArrayRef
                }
                DataType::Float64 => {
                    let values = extract_row_values::<f64>(&recs, field_index);
                    Arc::new(Float64Array::from(values)) as ArrayRef
                }
                DataType::Utf8 => {
                    let values: Vec<Option<String>> = recs
                        .iter()
                        .map(|row| {
                            let v2: Option<&[u8]> = row.try_get(field_index).ok();
                            v2.and_then(|v| String::from_utf8(Vec::from(v)).ok())
                        })
                        .collect();
                    let strs: Vec<_> = values.iter().map(|v| v.as_deref()).collect();
                    Arc::new(StringArray::from(strs)) as ArrayRef
                }
                dtype => {
                    panic!("Unsupported schema type {:?}", dtype)
                }
            };
            columns.push(array)
        }

        // Build record batch
        let schema_ref: SchemaRef = Arc::new(schema.clone());
        let batch = RecordBatch::try_new(schema_ref.clone(), columns)?;
        VegaFusionTable::try_new(schema_ref.clone(), vec![batch])
    }

    fn tables(&self) -> Result<HashMap<String, Schema>> {
        Ok(vec![(self.table.clone(), self.schema.clone())].into_iter().collect())
    }
}



/// Generic helper to extract row values at an index
fn extract_row_values<'a, T>(recs: &'a Vec<SqliteRow>, field_index: usize) -> Vec<Option<T>>
where T: sqlx::Decode<'a, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>
{
    let values: Vec<Option<T>> = recs
        .iter()
        .map(|row| row.try_get(field_index).ok())
        .collect();
    values
}