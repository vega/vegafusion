use std::collections::HashMap;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use vegafusion_core::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use vegafusion_core::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use vegafusion_core::arrow::record_batch::RecordBatch;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, VegaFusionError};
use async_trait::async_trait;
use regex::Regex;
use sqlgen::ast::Query;
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlx::sqlite::SqliteRow;
use crate::connection::SqlDatabaseConnection;


#[derive(Clone, Debug)]
pub struct SqLiteConnection {
    pub pool: Arc<SqlitePool>,
    pub dialect: Dialect,
}

impl SqLiteConnection {
    // Also input a table name (regular or temporary) to use as the source
    pub fn new(pool: Arc<SqlitePool>) -> Self {
        Self {
            pool,
            dialect: Dialect::sqlite(),
        }
    }
}

#[async_trait]
impl SqlDatabaseConnection for SqLiteConnection {
    async fn fetch_query(&self, query: &str, schema: &Schema) -> Result<VegaFusionTable> {
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

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    async fn tables(&self) -> Result<HashMap<String, Schema>> {
        let recs = sqlx::query(
            r#"
            SELECT name, sql FROM sqlite_schema
            WHERE type='table'
            ORDER BY name;
        "#,
        )
            .fetch_all(self.pool.clone().as_ref())
            .await
            .map_err(|err| {
                VegaFusionError::internal("Failed to query sqlite schema")
            })?;

        let re_pair = Regex::new(r#"("\w+"\s*\w+,?)"#).unwrap();
        let re_field = Regex::new(r#""(\w+)" (\w+),?"#).unwrap();

        let mut schemas: HashMap<String, Schema> = HashMap::new();
        for rec in recs {
            let mut fields = Vec::new();
            let table_name: &str = rec.get(0);
            let create_str: &str = rec.get(1);
            println!("{:?}", create_str);

            for v in re_pair.find_iter(create_str) {
                let field = re_field.captures(v.as_str()).unwrap();
                let col = field.get(1).unwrap().as_str().to_string();
                let typ = field.get(2).unwrap().as_str().to_string();
                let df_type = sqlite_to_arrow_dtype(&typ)?;
                fields.push(Field::new(&col, df_type, true));
            }

            let schema = Schema::new(fields);
            schemas.insert(table_name.to_string(), schema);
        }
        Ok(schemas)
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

fn sqlite_to_arrow_dtype(sql_type: &str) -> Result<DataType> {
    match sql_type {
        "INTEGER" => Ok(DataType::Int64),
        "REAL" => Ok(DataType::Float64),
        "TEXT" => Ok(DataType::Utf8),
        _ => Err(VegaFusionError::internal(format!(
            "Unsupported sql type: {}",
            sql_type
        ))),
    }
}
