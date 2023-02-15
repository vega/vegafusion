#[macro_use]
extern crate lazy_static;

mod utils;
use utils::{TOKIO_RUNTIME, make_connection, check_dataframe_query};
use datafusion_expr::{avg, col, count, expr, max, min, sum, Expr};
use rstest::rstest;
use serde_json::json;
use std::ops::{Div, Mul};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;


#[cfg(test)]
mod test_simple_aggs {
    use crate::*;

    #[rstest(
    dialect_name,
    case("athena"),
    case("bigquery"),
    case("clickhouse"),
    case("databricks"),
    case("datafusion"),
    case("dremio"),
    case("duckdb"),
    case("mysql"),
    case("postgres"),
    case("redshift"),
    case("snowflake"),
    case("sqlite")
    )]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": 2, "c": "A"},
                {"a": 3, "b": 2, "c": "BB"},
                {"a": 5, "b": 3, "c": "CCC"},
                {"a": 7, "b": 3, "c": "DDDD"},
                {"a": 9, "b": 3, "c": "EEEEE"},
                {"a": 11, "b": 3, "c": "FFFFFF"},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df
            .joinaggregate(
                vec![col("b")],
                vec![
                    min(col("a")).alias("min_a"),
                    max(col("a")).alias("max_a"),
                    avg(col("a")).alias("avg_a"),
                    sum(col("a")).alias("sum_a"),
                    count(col("a")).alias("count_a"),
                ],
            )
            .and_then(|df| {
                df.sort(
                    vec![Expr::Sort(expr::Sort {
                        expr: Box::new(col("a")),
                        asc: true,
                        nulls_first: true,
                    })],
                    None,
                )
            });

        check_dataframe_query(
            df_result,
            "joinaggregate",
            "simple_aggs",
            dialect_name,
            evaluable,
        );
    }
}

#[cfg(test)]
mod test_simple_aggs_no_grouping {
    use crate::*;

    #[rstest(
    dialect_name,
    case("athena"),
    case("bigquery"),
    case("clickhouse"),
    case("databricks"),
    case("datafusion"),
    case("dremio"),
    case("duckdb"),
    case("mysql"),
    case("postgres"),
    case("redshift"),
    case("snowflake"),
    case("sqlite")
    )]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": 2, "c": "A"},
                {"a": 3, "b": 2, "c": "BB"},
                {"a": 5, "b": 3, "c": "CCC"},
                {"a": 7, "b": 3, "c": "DDDD"},
                {"a": 9, "b": 3, "c": "EEEEE"},
                {"a": 11, "b": 3, "c": "FFFFFF"},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df
            .joinaggregate(
                vec![],
                vec![
                    min(col("a")).alias("min_a"),
                    max(col("a")).alias("max_a"),
                    avg(col("a")).alias("avg_a"),
                    sum(col("a")).alias("sum_a"),
                    count(col("a")).alias("count_a"),
                ],
            )
            .and_then(|df| {
                df.sort(
                    vec![Expr::Sort(expr::Sort {
                        expr: Box::new(col("a")),
                        asc: true,
                        nulls_first: true,
                    })],
                    None,
                )
            });

        check_dataframe_query(
            df_result,
            "joinaggregate",
            "simple_aggs_no_grouping",
            dialect_name,
            evaluable,
        );
    }
}