use crate::data::table::VegaFusionTableUtils;
use crate::sql::connection::SqlConnection;
use datafusion::prelude::SessionContext;
use log::Level;
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

    async fn fetch_query(
        &self,
        query: &str,
        _schema: &Schema,
    ) -> vegafusion_core::error::Result<VegaFusionTable> {
        info!("{}", query);
        let df = self.ctx.sql(query).await?;
        let res = VegaFusionTable::from_dataframe(df).await?;
        if log_enabled!(Level::Debug) {
            debug!("\n{}", res.pretty_format(Some(5)).unwrap());
            debug!("{:?}", res.schema);
        }
        Ok(res)
    }

    async fn tables(&self) -> vegafusion_core::error::Result<HashMap<String, Schema>> {
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
