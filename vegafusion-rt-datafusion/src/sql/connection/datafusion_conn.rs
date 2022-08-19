use crate::data::table::VegaFusionTableUtils;
use crate::sql::connection::SqlConnection;
use datafusion::prelude::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;

use sqlgen::dialect::Dialect;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;

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
    dialect.functions.insert("isvalid".to_string());
    dialect.functions.insert("isfinite".to_string());
    dialect
}

#[async_trait::async_trait]
impl SqlConnection for DataFusionConnection {
    fn id(&self) -> String {
        "datafusion".to_string()
    }

    async fn fetch_query(
        &self,
        query: &str,
        _schema: &Schema,
    ) -> vegafusion_core::error::Result<VegaFusionTable> {
        println!("datafusion query: {}", query);
        let df = self.ctx.sql(query).await?;
        VegaFusionTable::from_dataframe(df).await
    }

    async fn tables(&self) -> vegafusion_core::error::Result<HashMap<String, Schema>> {
        let state = self.ctx.as_ref().state.clone();
        let state = state.read();
        let catalog_names = state.catalog_list.catalog_names();
        let first_catalog_name = catalog_names.get(0).unwrap();
        let catalog = state.catalog_list.catalog(first_catalog_name).unwrap();

        let schema_provider_names = catalog.schema_names();
        let first_schema_provider_name = schema_provider_names.get(0).unwrap();
        let schema_provider = catalog.schema(first_schema_provider_name).unwrap();

        Ok(schema_provider
            .table_names()
            .iter()
            .map(|name| {
                let schema = schema_provider
                    .table(name)
                    .unwrap()
                    .schema()
                    .as_ref()
                    .clone();
                (name.clone(), schema)
            })
            .collect())
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }
}
