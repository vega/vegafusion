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
mod test_numeric_operators {
    use std::ops::{Add, Div, Mul, Sub};
    use datafusion_expr::{col, Expr, expr, lit};
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": 2},
                {"a": 3, "b": 4},
                {"a": 6, "b": 6},
                {"a": 9, "b": 8},
                {"a": 12, "b": 10},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df.select(vec![
            col("a"),
            col("b"),
            col("a").add(col("b")).alias("add"),
            col("a").sub(col("b")).alias("sub"),
            col("a").mul(col("b")).alias("mul"),
            col("a").div(lit(2)).alias("div"),
            col("a").modulus(lit(4)).alias("mod"),
            col("a").eq(col("b")).alias("eq"),
            col("a").not_eq(col("b")).alias("neq"),
            col("a").gt(lit(5)).alias("gt"),
            col("a").gt_eq(lit(5)).alias("gte"),
            col("b").lt(lit(6)).alias("lt"),
            col("b").lt_eq(lit(6)).alias("lte"),
            Expr::Negative(Box::new(col("a"))).alias("neg"),
        ]).and_then(|df| {
            df.sort(vec![
                Expr::Sort(expr::Sort {
                    expr: Box::new(col("a")),
                    asc: true,
                    nulls_first: true,
                })
            ], None)
        });

        check_dataframe_query(df_result, "select", "numeric_operators", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_logical_operators {
    use std::ops::{Add, Div, Mul, Sub};
    use datafusion_expr::{col, Expr, expr, lit};
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"i": 1, "a": true, "b": true},
                {"i": 2, "a": true, "b": false},
                {"i": 3, "a": false, "b": true},
                {"i": 4, "a": false, "b": false},
                {"i": 5, "a": false, "b": true},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df.select(vec![
            col("i"),
            col("a"),
            col("b"),
            col("a").or(col("b")).alias("or"),
            col("a").or(lit(true)).alias("or2"),
            col("a").and(col("b")).alias("and"),
            col("a").and(lit(true)).alias("and2"),
            col("a").not().alias("not"),
            col("a").eq(col("b")).alias("eq"),
            col("a").not_eq(col("b")).alias("neq"),
        ]).and_then(|df| {
            df.sort(vec![
                Expr::Sort(expr::Sort {
                    expr: Box::new(col("i")),
                    asc: true,
                    nulls_first: true,
                })
            ], None)
        });

        check_dataframe_query(df_result, "select", "logical_operators", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}


#[cfg(test)]
mod test_between {
    use std::ops::{Add, Div, Mul, Sub};
    use datafusion_expr::{col, Expr, expr, lit};
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": 2},
                {"a": 3, "b": 4},
                {"a": 6, "b": 6},
                {"a": 9, "b": 8},
                {"a": 12, "b": 10},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df.select(vec![
            col("a"),
            col("b"),
            Expr::Between(expr::Between {
                expr: Box::new(col("a")),
                negated: false,
                low: Box::new(lit(0)),
                high: Box::new(col("b")),
            }).alias("bet1"),
            Expr::Between(expr::Between {
                expr: Box::new(col("a")),
                negated: true,
                low: Box::new(lit(0)),
                high: Box::new(col("b")),
            }).alias("nbet1")
        ]).and_then(|df| {
            df.sort(vec![
                Expr::Sort(expr::Sort {
                    expr: Box::new(col("a")),
                    asc: true,
                    nulls_first: true,
                })
            ], None)
        });

        check_dataframe_query(df_result, "select", "between", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}


#[cfg(test)]
mod test_cast_numeric {
    use std::ops::{Add, Div, Mul, Sub};
    use arrow::datatypes::DataType;
    use datafusion_expr::{cast, col, Expr, expr, lit, when};
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": "0"},
                {"a": "1"},
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df.select(vec![
            col("a"),
            cast(col("a"), DataType::Int8).alias("i8"),
            cast(col("a"), DataType::UInt8).alias("u8"),
            cast(col("a"), DataType::Int16).alias("i16"),
            cast(col("a"), DataType::UInt16).alias("u16"),
            cast(col("a"), DataType::Int32).alias("i32"),
            cast(col("a"), DataType::UInt32).alias("u32"),
            cast(col("a"), DataType::Int64).alias("i64"),
            cast(col("a"), DataType::Float16).alias("f16"),
            cast(col("a"), DataType::Float32).alias("f32"),
            cast(col("a"), DataType::Float64).alias("f64"),
        ]).and_then(|df| {
            df.sort(vec![
                Expr::Sort(expr::Sort {
                    expr: Box::new(col("a")),
                    asc: true,
                    nulls_first: true,
                })
            ], None)
        });

        check_dataframe_query(df_result, "select", "cast_numeric", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
