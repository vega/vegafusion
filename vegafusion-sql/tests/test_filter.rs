#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_expr::{expr, lit, Expr};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use std::ops::Add;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_simple_gte {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");

        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "CCC"},
            {"a": 7, "b": 8, "c": "DDDD"},
            {"a": 9, "b": 10, "c": "EEEEE"},
            {"a": 11, "b": 12, "c": "FFFFFF"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df = df
            .filter(
                (flat_col("a").add(lit(2)).gt_eq(lit(9))).or((flat_col("b") % lit(4)).eq(lit(0))),
            )
            .await
            .unwrap();
        let df_result = df
            .sort(
                vec![Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col("a")),
                    asc: true,
                    nulls_first: true,
                })],
                None,
            )
            .await;

        check_dataframe_query(df_result, "filter", "simple_gte", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
