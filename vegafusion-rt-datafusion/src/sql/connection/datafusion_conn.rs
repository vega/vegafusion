use crate::data::table::VegaFusionTableUtils;
use crate::sql::connection::SqlConnection;
use datafusion::prelude::SessionContext;
use log::Level;
use std::collections::HashMap;
use std::sync::Arc;

use crate::expression::compiler::utils::cast_to;
use crate::expression::escape::flat_col;
use sqlgen::dialect::Dialect;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::Result;

#[derive(Clone)]
pub struct DataFusionConnection {
    dialect: Dialect,
    ctx: Arc<SessionContext>,
}

impl DataFusionConnection {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self {
            dialect: make_datafusion_dialect(),
            ctx,
        }
    }
}

pub fn make_datafusion_dialect() -> Dialect {
    let mut dialect = Dialect::datafusion();
    let functions = &mut dialect.functions;

    functions.insert("isnan".to_string());
    functions.insert("isfinite".to_string());

    // datetime
    functions.insert("date_to_timestamptz".to_string());
    functions.insert("timestamp_to_timestamptz".to_string());
    functions.insert("timestamptz_to_timestamp".to_string());
    functions.insert("epoch_ms_to_timestamptz".to_string());
    functions.insert("str_to_timestamptz".to_string());
    functions.insert("make_timestamptz".to_string());
    functions.insert("timestamptz_to_epoch_ms".to_string());

    // timeunit
    functions.insert("vega_timeunit".to_string());

    // timeformat
    functions.insert("format_timestamp".to_string());

    // math
    functions.insert("pow".to_string());

    // list
    functions.insert("make_list".to_string());
    functions.insert("len".to_string());
    functions.insert("indexof".to_string());

    dialect
}

#[async_trait::async_trait]
impl SqlConnection for DataFusionConnection {
    fn id(&self) -> String {
        "datafusion".to_string()
    }

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

        let res = VegaFusionTable::from_dataframe(df).await?;
        if log_enabled!(Level::Debug) {
            debug!("\n{}", res.pretty_format(Some(5)).unwrap());
            debug!("{:?}", res.schema);
        }
        Ok(res)
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

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }
}
