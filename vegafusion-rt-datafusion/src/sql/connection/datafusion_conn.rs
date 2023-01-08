use crate::data::table::VegaFusionTableUtils;
use crate::sql::connection::SqlConnection;
use datafusion::prelude::SessionContext;
use log::Level;
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;

use sqlgen::dialect::Dialect;
use vegafusion_core::arrow::datatypes::Schema;
use vegafusion_core::data::table::VegaFusionTable;

#[derive(Clone)]
pub struct DataFusionConnection {
    dialect: Dialect,
    ctx: Arc<SessionContext>,
    table_names: Vec<String>,
}

impl DataFusionConnection {
    pub fn new(ctx: Arc<SessionContext>, table_names: Vec<String>) -> Self {
        Self {
            dialect: make_datafusion_dialect(),
            ctx,
            table_names
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
        let state = self.ctx.as_ref().state().clone();
        self.table_names
            .iter()
            .map(|name| {
                let table_ref = TableReference::Bare { table: name.as_str() };
                let table = state.get_table_provider(table_ref)?;
                let schema = table.schema().as_ref().clone();
                Ok((name.clone(), schema))
            })
            .collect::<vegafusion_core::error::Result<HashMap<_, _>>>()
    }

    fn dialect(&self) -> &Dialect {
        &self.dialect
    }
}
