#[macro_use]
extern crate lazy_static;

mod utils;
use utils::{TOKIO_RUNTIME, make_connection, check_dataframe_query};
use datafusion_expr::{col, expr, lit, Expr};
use rstest::rstest;
use serde_json::json;
use std::ops::Add;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;


#[cfg(test)]
mod test_simple_gte {
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
                {"a": 3, "b": 4, "c": "BB"},
                {"a": 5, "b": 6, "c": "CCC"},
                {"a": 7, "b": 8, "c": "DDDD"},
                {"a": 9, "b": 10, "c": "EEEEE"},
                {"a": 11, "b": 12, "c": "FFFFFF"},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df = df
            .filter((col("a").add(lit(2)).gt_eq(lit(9))).or(col("b").modulus(lit(4)).eq(lit(0))))
            .unwrap();
        let df_result = df.sort(
            vec![Expr::Sort(expr::Sort {
                expr: Box::new(col("a")),
                asc: true,
                nulls_first: true,
            })],
            None,
        );

        check_dataframe_query(df_result, "filter", "simple_gte", dialect_name, evaluable);
    }
}
