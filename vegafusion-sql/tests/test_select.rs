#[macro_use]
extern crate lazy_static;

mod utils;
use rstest::rstest;
use rstest_reuse::{self, *};
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};

use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_datafusion_udfs::udfs::{
    datetime::date_add_tz::DATE_ADD_TZ_UDF, math::isfinite::ISFINITE_UDF,
};
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_numeric_operators {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::ops::{Add, Div, Mul, Sub};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
            {"a": 6, "b": 6},
            {"a": 9, "b": 8},
            {"a": 12, "b": 10},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("a").add(flat_col("b")).alias("add"),
                flat_col("a").sub(flat_col("b")).alias("sub"),
                flat_col("a").mul(flat_col("b")).alias("mul"),
                flat_col("a").div(lit(2)).alias("div"),
                (flat_col("a") % lit(4)).alias("mod"),
                flat_col("a").eq(flat_col("b")).alias("eq"),
                flat_col("a").not_eq(flat_col("b")).alias("neq"),
                flat_col("a").gt(lit(5)).alias("gt"),
                flat_col("a").gt_eq(lit(5)).alias("gte"),
                flat_col("b").lt(lit(6)).alias("lt"),
                flat_col("b").lt_eq(lit(6)).alias("lte"),
                Expr::Negative(Box::new(flat_col("a"))).alias("neg"),
            ])
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
            "select",
            "numeric_operators",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_logical_operators {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"i": 1, "a": true, "b": true},
            {"i": 2, "a": true, "b": false},
            {"i": 3, "a": false, "b": true},
            {"i": 4, "a": false, "b": false},
            {"i": 5, "a": false, "b": true},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("i"),
                flat_col("a"),
                flat_col("b"),
                flat_col("a").or(flat_col("b")).alias("or"),
                flat_col("a").or(lit(true)).alias("or2"),
                flat_col("a").and(flat_col("b")).alias("and"),
                flat_col("a").and(lit(true)).alias("and2"),
                (!flat_col("a")).alias("not"),
                flat_col("a").eq(flat_col("b")).alias("eq"),
                flat_col("a").not_eq(flat_col("b")).alias("neq"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col("i")),
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
            "select",
            "logical_operators",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_between {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
            {"a": 6, "b": 6},
            {"a": 9, "b": 8},
            {"a": 12, "b": 10},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::Between(expr::Between {
                    expr: Box::new(flat_col("a")),
                    negated: false,
                    low: Box::new(lit(0)),
                    high: Box::new(flat_col("b")),
                })
                .alias("bet1"),
                Expr::Between(expr::Between {
                    expr: Box::new(flat_col("a")),
                    negated: true,
                    low: Box::new(lit(0)),
                    high: Box::new(flat_col("b")),
                })
                .alias("nbet1"),
            ])
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

        check_dataframe_query(df_result, "select", "between", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_cast_numeric {
    use crate::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{cast, expr, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": "0"},
            {"a": "1"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                cast(flat_col("a"), DataType::Int8).alias("i8"),
                cast(flat_col("a"), DataType::UInt8).alias("u8"),
                cast(flat_col("a"), DataType::Int16).alias("i16"),
                cast(flat_col("a"), DataType::UInt16).alias("u16"),
                cast(flat_col("a"), DataType::Int32).alias("i32"),
                cast(flat_col("a"), DataType::UInt32).alias("u32"),
                cast(flat_col("a"), DataType::Int64).alias("i64"),
                cast(flat_col("a"), DataType::Float32).alias("f32"),
                cast(flat_col("a"), DataType::Float64).alias("f64"),
            ])
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

        check_dataframe_query(df_result, "select", "cast_numeric", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_try_cast_numeric {
    use crate::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{expr, try_cast, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": "0"},
            {"a": "1"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                try_cast(flat_col("a"), DataType::Int8).alias("i8"),
                try_cast(flat_col("a"), DataType::UInt8).alias("u8"),
                try_cast(flat_col("a"), DataType::Int16).alias("i16"),
                try_cast(flat_col("a"), DataType::UInt16).alias("u16"),
                try_cast(flat_col("a"), DataType::Int32).alias("i32"),
                try_cast(flat_col("a"), DataType::UInt32).alias("u32"),
                try_cast(flat_col("a"), DataType::Int64).alias("i64"),
                try_cast(flat_col("a"), DataType::Float32).alias("f32"),
                try_cast(flat_col("a"), DataType::Float64).alias("f64"),
            ])
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
            "select",
            "try_cast_numeric",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_cast_string {
    use crate::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{cast, expr, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": null, "c": true, "d": "A"},
            {"a": 1, "b": 1.5, "c": false, "d": "BB"},
            {"a": null, "b": 2.25, "c": null, "d": "CCC"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                cast(flat_col("a"), DataType::Utf8).alias("a"),
                cast(flat_col("b"), DataType::Utf8).alias("b"),
                cast(flat_col("c"), DataType::Utf8).alias("c"),
                cast(flat_col("d"), DataType::Utf8).alias("d"),
            ])
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

        check_dataframe_query(df_result, "select", "cast_string", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_try_cast_string {
    use crate::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{expr, try_cast, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": null, "c": true, "d": "A"},
            {"a": 1, "b": 1.5, "c": false, "d": "BB"},
            {"a": null, "b": 2.25, "c": null, "d": "CCC"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                try_cast(flat_col("a"), DataType::Utf8).alias("a"),
                try_cast(flat_col("b"), DataType::Utf8).alias("b"),
                try_cast(flat_col("c"), DataType::Utf8).alias("c"),
                try_cast(flat_col("d"), DataType::Utf8).alias("d"),
            ])
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
            "select",
            "try_cast_string",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_non_finite_numbers {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                lit(f64::NEG_INFINITY).alias("ninf"),
                lit(f64::NAN).alias("nan"),
                lit(f64::INFINITY).alias("inf"),
            ])
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
            "select",
            "non_finite_numbers",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_scalar_math_functions {
    use crate::*;
    use datafusion_expr::{expr, BuiltinScalarFunction, Expr};
    use vegafusion_common::column::flat_col;

    fn make_scalar_fn1(fun: BuiltinScalarFunction, arg: &str, alias: &str) -> Expr {
        Expr::ScalarFunction(expr::ScalarFunction {
            fun,
            args: vec![flat_col(arg)],
        })
        .alias(alias)
    }

    fn make_scalar_fn2(fun: BuiltinScalarFunction, arg1: &str, arg2: &str, alias: &str) -> Expr {
        Expr::ScalarFunction(expr::ScalarFunction {
            fun,
            args: vec![flat_col(arg1), flat_col(arg2)],
        })
        .alias(alias)
    }

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, _evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": -1.8, "c": 0.1},
            {"a": 1, "b": -1, "c": 0.2},
            {"a": 2, "b": 0, "c": 0.4},
            {"a": 3, "b": 1, "c": 0.6},
            {"a": 4, "b": 1.8 ,"c": 0.8},
            {"a": 5, "b": null, "c": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                make_scalar_fn1(BuiltinScalarFunction::Abs, "b", "abs"),
                make_scalar_fn1(BuiltinScalarFunction::Acos, "c", "acos"),
                make_scalar_fn1(BuiltinScalarFunction::Asin, "c", "asin"),
                make_scalar_fn1(BuiltinScalarFunction::Atan, "c", "atan"),
                make_scalar_fn2(BuiltinScalarFunction::Atan2, "c", "a", "atan2"),
                make_scalar_fn1(BuiltinScalarFunction::Ceil, "b", "ceil"),
                make_scalar_fn1(BuiltinScalarFunction::Cos, "b", "cos"),
                make_scalar_fn1(BuiltinScalarFunction::Exp, "b", "exp"),
                make_scalar_fn1(BuiltinScalarFunction::Floor, "b", "floor"),
                make_scalar_fn1(BuiltinScalarFunction::Ln, "c", "ln"),
                make_scalar_fn1(BuiltinScalarFunction::Log, "c", "log"),
                make_scalar_fn1(BuiltinScalarFunction::Log10, "c", "log10"),
                make_scalar_fn1(BuiltinScalarFunction::Log2, "c", "log2"),
                make_scalar_fn2(BuiltinScalarFunction::Power, "b", "a", "power"),
                make_scalar_fn1(BuiltinScalarFunction::Round, "b", "round"),
                make_scalar_fn1(BuiltinScalarFunction::Sin, "b", "sin"),
                make_scalar_fn1(BuiltinScalarFunction::Sqrt, "c", "sqrt"),
                make_scalar_fn1(BuiltinScalarFunction::Tan, "b", "tan"),
            ])
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
            "select",
            "scalar_math_functions",
            dialect_name,
            false, // Don't check result due to floating point differences
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_is_finite {
    use crate::*;
    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_expr::{expr, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        // Build Record batch manually so we can include non-finite values
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, true),
        ])) as SchemaRef;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![
                    0.0,
                    -1.5,
                    f64::NEG_INFINITY,
                    f64::INFINITY,
                    f64::NAN,
                ])),
            ],
        )
        .unwrap();
        let table = VegaFusionTable::try_new(schema, vec![batch]).unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(ISFINITE_UDF.clone()),
                    args: vec![flat_col("a")],
                })
                .alias("f1"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(ISFINITE_UDF.clone()),
                    args: vec![flat_col("b")],
                })
                .alias("f2"),
            ])
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

        check_dataframe_query(df_result, "select", "is_finite", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_str_to_utc_timestamp {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": "2022-01-01 12:34:56"},
            {"a": 1, "b": "2022-01-02 02:30:01"},
            {"a": 2, "b": "2022-01-03 01:42:21"},
            {"a": 3, "b": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("America/New_York")],
                })
                .alias("b_utc"),
            ])
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
            "select",
            "str_to_utc_timestamp",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_date_part_tz {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::date_part_tz::DATE_PART_TZ_UDF;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": "2022-01-01 12:34:56"},
            {"a": 1, "b": "2022-01-02 02:30:01"},
            {"a": 2, "b": "2022-01-03 01:42:21"},
            {"a": 3, "b": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("America/New_York")],
                })
                .alias("b_utc"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("b_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_PART_TZ_UDF.clone()),
                    args: vec![lit("hour"), flat_col("b_utc"), lit("UTC")],
                })
                .alias("hours_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_PART_TZ_UDF.clone()),
                    args: vec![lit("hour"), flat_col("b_utc"), lit("America/Los_Angeles")],
                })
                .alias("hours_la"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_PART_TZ_UDF.clone()),
                    args: vec![lit("hour"), flat_col("b_utc"), lit("America/New_York")],
                })
                .alias("hours_nyc"),
            ])
            .await
        } else {
            df_result
        };

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
            "select",
            "test_date_part_tz",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_date_trunc_tz {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::date_trunc_tz::DATE_TRUNC_TZ_UDF;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": "2022-01-01 12:34:56"},
            {"a": 1, "b": "2022-01-02 02:30:01"},
            {"a": 2, "b": "2022-01-03 01:42:21"},
            {"a": 3, "b": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("America/New_York")],
                })
                .alias("b_utc"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("b_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_TRUNC_TZ_UDF.clone()),
                    args: vec![lit("day"), flat_col("b_utc"), lit("UTC")],
                })
                .alias("day_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_TRUNC_TZ_UDF.clone()),
                    args: vec![lit("day"), flat_col("b_utc"), lit("America/Los_Angeles")],
                })
                .alias("day_la"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_TRUNC_TZ_UDF.clone()),
                    args: vec![lit("day"), flat_col("b_utc"), lit("America/New_York")],
                })
                .alias("day_nyc"),
            ])
            .await
        } else {
            df_result
        };

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
            "select",
            "test_date_trunc_tz",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_make_timestamp_tz {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::make_utc_timestamp::MAKE_UTC_TIMESTAMP;
    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "Y": 2001, "M": 0, "d": 1, "h": 3, "min": 2, "s": 32, "ms": 123},
            {"a": 2, "Y": 1984, "M": 3, "d": 12, "h": 7, "min": 0, "s": 0, "ms": 0},
            {"a": 3, "Y": 1968, "M": 11, "d": 30, "h": 18, "min": 43, "s": 58, "ms": 18},
            {"a": 4, "Y": null, "M": null, "d": null, "h": null, "min": null, "s": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(MAKE_UTC_TIMESTAMP.clone()),
                    args: vec![
                        flat_col("Y"),
                        flat_col("M"),
                        flat_col("d"),
                        flat_col("h"),
                        flat_col("min"),
                        flat_col("s"),
                        flat_col("ms"),
                        lit("UTC"),
                    ],
                })
                .alias("ts_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(MAKE_UTC_TIMESTAMP.clone()),
                    args: vec![
                        flat_col("Y"),
                        flat_col("M"),
                        flat_col("d"),
                        flat_col("h"),
                        flat_col("min"),
                        flat_col("s"),
                        flat_col("ms"),
                        lit("America/New_York"),
                    ],
                })
                .alias("ts_nyc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(MAKE_UTC_TIMESTAMP.clone()),
                    args: vec![
                        flat_col("Y"),
                        flat_col("M"),
                        flat_col("d"),
                        flat_col("h"),
                        flat_col("min"),
                        flat_col("s"),
                        flat_col("ms"),
                        lit("America/Los_Angeles"),
                    ],
                })
                .alias("ts_la"),
            ])
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
            "select",
            "test_make_timestamp_tz",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_epoch_to_utc_timestamp {
    use crate::*;
    use datafusion_expr::{expr, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::epoch_to_utc_timestamp::EPOCH_MS_TO_UTC_TIMESTAMP_UDF;
    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "t": 1641058496123i64},
            {"a": 2, "t": 1641108601321i64},
            {"a": 3, "t": 1641192141999i64},
            {"a": 4, "t": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("t"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(EPOCH_MS_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("t")],
                })
                .alias("t_utc"),
            ])
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
            "select",
            "test_epoch_to_utc_timestamp",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_utc_timestamp_to_epoch_ms {
    use crate::*;
    use datafusion_expr::{expr, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::epoch_to_utc_timestamp::EPOCH_MS_TO_UTC_TIMESTAMP_UDF;
    use vegafusion_datafusion_udfs::udfs::datetime::utc_timestamp_to_epoch::UTC_TIMESTAMP_TO_EPOCH_MS;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "t": 1641058496123i64},
            {"a": 2, "t": 1641108601321i64},
            {"a": 3, "t": 1641192141999i64},
            {"a": 4, "t": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("t"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(EPOCH_MS_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("t")],
                })
                .alias("t_utc"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.select(vec![
                flat_col("a"),
                flat_col("t"),
                flat_col("t_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(UTC_TIMESTAMP_TO_EPOCH_MS.clone()),
                    args: vec![flat_col("t_utc")],
                })
                .alias("epoch_millis"),
            ])
            .await
        } else {
            df_result
        };

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
            "select",
            "test_utc_timestamp_to_epoch_ms",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_date_add_tz {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": "2022-03-01 03:34:56"},
            {"a": 1, "b": "2022-04-02 02:30:01"},
            {"a": 2, "b": "2022-05-03 01:42:21"},
            {"a": 3, "b": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("UTC")],
                })
                .alias("b_utc"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("b_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_ADD_TZ_UDF.clone()),
                    args: vec![lit("month"), lit(1), flat_col("b_utc"), lit("UTC")],
                })
                .alias("month_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_ADD_TZ_UDF.clone()),
                    args: vec![
                        lit("month"),
                        lit(1),
                        flat_col("b_utc"),
                        lit("America/New_York"),
                    ],
                })
                .alias("month_nyc"),
            ])
            .await
        } else {
            df_result
        };

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
            "select",
            "test_date_add_tz",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_utc_timestamp_to_str {
    use crate::*;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;
    use vegafusion_datafusion_udfs::udfs::datetime::utc_timestamp_to_str::UTC_TIMESTAMP_TO_STR_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": "2022-03-01 03:34:56.123"},
            {"a": 1, "b": "2022-04-02 02:30:01.321"},
            {"a": 2, "b": "2022-05-03 01:42:21"},
            {"a": 3, "b": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("UTC")],
                })
                .alias("b_utc"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("b_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(UTC_TIMESTAMP_TO_STR_UDF.clone()),
                    args: vec![flat_col("b_utc"), lit("UTC")],
                })
                .alias("str_utc"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(UTC_TIMESTAMP_TO_STR_UDF.clone()),
                    args: vec![flat_col("b_utc"), lit("America/New_York")],
                })
                .alias("str_nyc"),
            ])
            .await
        } else {
            df_result
        };

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
            "select",
            "test_utc_timestamp_to_str",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_date_to_utc_timestamp {
    use crate::*;
    use arrow::array::{ArrayRef, Date32Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::date_to_utc_timestamp::DATE_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let schema_ref: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Date32, true),
        ]));
        let columns = vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            Arc::new(Date32Array::from(vec![
                10580, // 1998-12-20
                10980, // 2000-01-24
                11000, // 2000-02-13
                12000, // 2002-11-09
            ])) as ArrayRef,
        ];

        let batch = RecordBatch::try_new(schema_ref.clone(), columns).unwrap();
        let table = VegaFusionTable::try_new(schema_ref, vec![batch]).unwrap();
        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(DATE_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("America/New_York")],
                })
                .alias("b_utc"),
            ])
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
            "select",
            "date_to_utc_timestamp",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_timestamp_to_utc_timestamp {
    use crate::*;
    use arrow::array::{ArrayRef, Int32Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion_expr::{expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_common::column::flat_col;
    use vegafusion_datafusion_udfs::udfs::datetime::to_utc_timestamp::TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let schema_ref: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let columns = vec![
            Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![
                1641058496123i64, // 2022-01-01 05:34:56.123
                1641108601321i64, // 2022-01-02 07:30:01.321
                1641192141999i64, // 2022-01-03 06:42:21.999
            ])) as ArrayRef,
        ];

        let batch = RecordBatch::try_new(schema_ref.clone(), columns).unwrap();
        let table = VegaFusionTable::try_new(schema_ref, vec![batch]).unwrap();
        let df_result = SqlDataFrame::from_values(&table, conn, Default::default());

        let df_result = if let Ok(df) = df_result {
            df.select(vec![
                flat_col("a"),
                flat_col("b"),
                Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new(TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![flat_col("b"), lit("America/New_York")],
                })
                .alias("b_utc"),
            ])
            .await
        } else {
            df_result
        };

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
            "select",
            "to_utc_timestamp",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_string_ops {
    use crate::*;
    use datafusion_expr::{expr, lit, BuiltinScalarFunction, Expr};
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 0, "b": "1234", "c": "efGH"},
            {"a": 1, "b": "abCD", "c": "5678"},
            {"a": 3, "b": null},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("c"),
                Expr::ScalarFunction(expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Substr,
                    args: vec![flat_col("b"), lit(2), lit(2)],
                })
                .alias("b_substr"),
                Expr::ScalarFunction(expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Concat,
                    args: vec![flat_col("b"), lit(" "), flat_col("c")],
                })
                .alias("bc_concat"),
                Expr::ScalarFunction(expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Upper,
                    args: vec![flat_col("b")],
                })
                .alias("b_upper"),
                Expr::ScalarFunction(expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Lower,
                    args: vec![flat_col("b")],
                })
                .alias("b_lower"),
            ])
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
            "select",
            "test_string_ops",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
