#[macro_use]
extern crate lazy_static;

mod utils;
use utils::{TOKIO_RUNTIME, make_connection, check_dataframe_query, dialect_names};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;


#[cfg(test)]
mod test_values1 {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": 2, "c": "A"},
                {"a": 3, "b": 4, "c": "BB"},
                {"a": 5, "b": 6, "c": "CCC"},
            ]),
            1024,
        )
            .unwrap();

        let df_result = SqlDataFrame::from_values(&table, conn);
        check_dataframe_query(df_result, "values", "values1", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
