#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_expr::{avg, count, expr, max, min, sum, Expr};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_simple_aggs {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 2, "c": "BB"},
            {"a": 5, "b": 3, "c": "CCC"},
            {"a": 7, "b": 3, "c": "DDDD"},
            {"a": 9, "b": 3, "c": "EEEEE"},
            {"a": 11, "b": 3, "c": "FFFFFF"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .joinaggregate(
                vec![flat_col("b")],
                vec![
                    min(flat_col("a")).alias("min_a"),
                    max(flat_col("a")).alias("max_a"),
                    avg(flat_col("a")).alias("avg_a"),
                    sum(flat_col("a")).alias("sum_a"),
                    count(flat_col("a")).alias("count_a"),
                ],
            )
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col("a")),
                    asc: true,
                    nulls_first: true,
                })],
                None,
            )
            .await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "joinaggregate",
            "simple_aggs",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_simple_aggs_no_grouping {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 2, "c": "BB"},
            {"a": 5, "b": 3, "c": "CCC"},
            {"a": 7, "b": 3, "c": "DDDD"},
            {"a": 9, "b": 3, "c": "EEEEE"},
            {"a": 11, "b": 3, "c": "FFFFFF"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .joinaggregate(
                vec![],
                vec![
                    min(flat_col("a")).alias("min_a"),
                    max(flat_col("a")).alias("max_a"),
                    avg(flat_col("a")).alias("avg_a"),
                    sum(flat_col("a")).alias("sum_a"),
                    count(flat_col("a")).alias("count_a"),
                ],
            )
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col("a")),
                    asc: true,
                    nulls_first: true,
                })],
                None,
            )
            .await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "joinaggregate",
            "simple_aggs_no_grouping",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
