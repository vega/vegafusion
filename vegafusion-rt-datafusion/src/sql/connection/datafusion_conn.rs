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
    let functions = &mut dialect.functions;

    functions.insert("isnan".to_string());
    functions.insert("isfinite".to_string());

    // datetime
    functions.insert("date_to_timestamptz".to_string());
    functions.insert("timestamp_to_timestamptz".to_string());
    functions.insert("epoch_to_timestamptz".to_string());
    functions.insert("str_to_timestamptz".to_string());
    functions.insert("make_timestamptz".to_string());

    functions.insert("vg_time".to_string());

    // date parts
    functions.insert("year_tz".to_string());
    functions.insert("month_tz".to_string());
    functions.insert("quarter_tz".to_string());
    functions.insert("date_tz".to_string());
    functions.insert("dayofyear_tz".to_string());
    functions.insert("day_tz".to_string());
    functions.insert("hours_tz".to_string());
    functions.insert("minutes_tz".to_string());
    functions.insert("seconds_tz".to_string());
    functions.insert("milliseconds_tz".to_string());

    // timeunit transform
    functions.insert("timeunit_start".to_string());
    functions.insert("timeunit_end".to_string());

    // timeformat
    functions.insert("vg_timeformat".to_string());

    // math
    functions.insert("pow".to_string());

    // list
    functions.insert("make_list".to_string());
    functions.insert("len".to_string());

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
        let res = VegaFusionTable::from_dataframe(df).await?;
        // println!("{}", res.pretty_format(Some(10)).unwrap());
        // println!("{:#?}", res.schema);
        Ok(res)
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
