#[macro_use]
extern crate lazy_static;

mod utils;
use rstest::rstest;
use rstest_reuse::{self, *};
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};

use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_datafusion_udfs::udfs::math::isfinite::ISFINITE_UDF;
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_numeric_operators {
    use crate::*;
    use datafusion_expr::{col, expr, lit, Expr};
    use std::ops::{Add, Div, Mul, Sub};

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
        let df_result = df
            .select(vec![
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
            ])
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
    use datafusion_expr::{col, expr, lit, Expr};

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
        let df_result = df
            .select(vec![
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
            ])
            .and_then(|df| {
                df.sort(
                    vec![Expr::Sort(expr::Sort {
                        expr: Box::new(col("i")),
                        asc: true,
                        nulls_first: true,
                    })],
                    None,
                )
            });

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
    use datafusion_expr::{col, expr, lit, Expr};

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
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                Expr::Between(expr::Between {
                    expr: Box::new(col("a")),
                    negated: false,
                    low: Box::new(lit(0)),
                    high: Box::new(col("b")),
                })
                .alias("bet1"),
                Expr::Between(expr::Between {
                    expr: Box::new(col("a")),
                    negated: true,
                    low: Box::new(lit(0)),
                    high: Box::new(col("b")),
                })
                .alias("nbet1"),
            ])
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

        check_dataframe_query(df_result, "select", "between", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_cast_numeric {
    use crate::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{cast, col, expr, Expr};

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
        let df_result = df
            .select(vec![
                col("a"),
                cast(col("a"), DataType::Int8).alias("i8"),
                cast(col("a"), DataType::UInt8).alias("u8"),
                cast(col("a"), DataType::Int16).alias("i16"),
                cast(col("a"), DataType::UInt16).alias("u16"),
                cast(col("a"), DataType::Int32).alias("i32"),
                cast(col("a"), DataType::UInt32).alias("u32"),
                cast(col("a"), DataType::Int64).alias("i64"),
                cast(col("a"), DataType::Float32).alias("f32"),
                cast(col("a"), DataType::Float64).alias("f64"),
            ])
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

        check_dataframe_query(df_result, "select", "cast_numeric", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_cast_string {
    use crate::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::{cast, col, expr, Expr};

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": null, "c": true, "d": "A"},
                {"a": 1, "b": 1.5, "c": false, "d": "BB"},
                {"a": null, "b": 2.25, "c": null, "d": "CCC"},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df
            .select(vec![
                cast(col("a"), DataType::Utf8).alias("a"),
                cast(col("b"), DataType::Utf8).alias("b"),
                cast(col("c"), DataType::Utf8).alias("c"),
                cast(col("d"), DataType::Utf8).alias("d"),
            ])
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

        check_dataframe_query(df_result, "select", "cast_string", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_non_finite_numbers {
    use crate::*;
    use datafusion_expr::{col, expr, lit, Expr};

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 0},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df
            .select(vec![
                col("a"),
                lit(f64::NEG_INFINITY).alias("ninf"),
                lit(f64::NAN).alias("nan"),
                lit(f64::INFINITY).alias("inf"),
            ])
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
    use datafusion_expr::{col, expr, BuiltinScalarFunction, Expr};

    fn make_scalar_fn1(fun: BuiltinScalarFunction, arg: &str, alias: &str) -> Expr {
        Expr::ScalarFunction {
            fun,
            args: vec![col(arg)],
        }
        .alias(alias)
    }

    fn make_scalar_fn2(fun: BuiltinScalarFunction, arg1: &str, arg2: &str, alias: &str) -> Expr {
        Expr::ScalarFunction {
            fun,
            args: vec![col(arg1), col(arg2)],
        }
        .alias(alias)
    }

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": -1.8, "c": 0.1},
                {"a": 1, "b": -1, "c": 0.2},
                {"a": 2, "b": 0, "c": 0.4},
                {"a": 3, "b": 1, "c": 0.6},
                {"a": 4, "b": 1.8 ,"c": 0.8},
                {"a": 5, "b": null, "c": null},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df_result = df
            .select(vec![
                col("a"),
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
            "select",
            "scalar_math_functions",
            dialect_name,
            evaluable,
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
    use datafusion_expr::{col, expr, Expr};
    use std::sync::Arc;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
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
        let table = VegaFusionTable::try_new(schema.clone(), vec![batch]).unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();

        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                Expr::ScalarUDF {
                    fun: Arc::new(ISFINITE_UDF.clone()),
                    args: vec![col("a")],
                }
                .alias("f1"),
                Expr::ScalarUDF {
                    fun: Arc::new(ISFINITE_UDF.clone()),
                    args: vec![col("b")],
                }
                .alias("f2"),
            ])
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

        check_dataframe_query(df_result, "select", "is_finite", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_str_to_utc_timestamp {
    use crate::*;
    use datafusion_expr::{col, expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": "2022-01-01 12:34:56"},
                {"a": 1, "b": "2022-01-02 02:30:01"},
                {"a": 2, "b": "2022-01-03 01:42:21"},
                {"a": 3, "b": null},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();

        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                Expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![col("b"), lit("America/New_York")],
                }
                .alias("b_utc"),
            ])
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
    use datafusion_expr::{col, expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_datafusion_udfs::udfs::datetime::date_part_tz::DATE_PART_TZ_UDF;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": "2022-01-01 12:34:56"},
                {"a": 1, "b": "2022-01-02 02:30:01"},
                {"a": 2, "b": "2022-01-03 01:42:21"},
                {"a": 3, "b": null},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();

        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                Expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![col("b"), lit("America/New_York")],
                }
                .alias("b_utc"),
            ])
            .and_then(|df| {
                df.select(vec![
                    col("a"),
                    col("b"),
                    col("b_utc"),
                    Expr::ScalarUDF {
                        fun: Arc::new(DATE_PART_TZ_UDF.clone()),
                        args: vec![lit("hour"), col("b_utc"), lit("UTC")],
                    }
                    .alias("hours_utc"),
                    Expr::ScalarUDF {
                        fun: Arc::new(DATE_PART_TZ_UDF.clone()),
                        args: vec![lit("hour"), col("b_utc"), lit("America/Los_Angeles")],
                    }
                    .alias("hours_la"),
                    Expr::ScalarUDF {
                        fun: Arc::new(DATE_PART_TZ_UDF.clone()),
                        args: vec![lit("hour"), col("b_utc"), lit("America/New_York")],
                    }
                    .alias("hours_nyc"),
                ])
            })
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
    use datafusion_expr::{col, expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_datafusion_udfs::udfs::datetime::date_trunc_tz::DATE_TRUNC_TZ_UDF;
    use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": "2022-01-01 12:34:56"},
                {"a": 1, "b": "2022-01-02 02:30:01"},
                {"a": 2, "b": "2022-01-03 01:42:21"},
                {"a": 3, "b": null},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();

        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                Expr::ScalarUDF {
                    fun: Arc::new(STR_TO_UTC_TIMESTAMP_UDF.clone()),
                    args: vec![col("b"), lit("America/New_York")],
                }
                .alias("b_utc"),
            ])
            .and_then(|df| {
                df.select(vec![
                    col("a"),
                    col("b"),
                    col("b_utc"),
                    Expr::ScalarUDF {
                        fun: Arc::new(DATE_TRUNC_TZ_UDF.clone()),
                        args: vec![lit("day"), col("b_utc"), lit("UTC")],
                    }
                    .alias("day_utc"),
                    Expr::ScalarUDF {
                        fun: Arc::new(DATE_TRUNC_TZ_UDF.clone()),
                        args: vec![lit("day"), col("b_utc"), lit("America/Los_Angeles")],
                    }
                    .alias("day_la"),
                    Expr::ScalarUDF {
                        fun: Arc::new(DATE_TRUNC_TZ_UDF.clone()),
                        args: vec![lit("day"), col("b_utc"), lit("America/New_York")],
                    }
                    .alias("day_nyc"),
                ])
            })
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
    use datafusion_expr::{col, expr, lit, Expr};
    use std::sync::Arc;
    use vegafusion_datafusion_udfs::udfs::datetime::make_utc_timestamp::MAKE_UTC_TIMESTAMP;
    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "Y": 2001, "M": 0, "d": 1, "h": 3, "min": 2, "s": 32, "ms": 123},
                {"a": 2, "Y": 1984, "M": 3, "d": 12, "h": 7, "min": 0, "s": 0, "ms": 0},
                {"a": 3, "Y": 1968, "M": 11, "d": 30, "h": 18, "min": 43, "s": 58, "ms": 18},
                {"a": 4, "Y": null, "M": null, "d": null, "h": null, "min": null, "s": null},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();

        let df_result = df
            .select(vec![
                col("a"),
                Expr::ScalarUDF {
                    fun: Arc::new(MAKE_UTC_TIMESTAMP.clone()),
                    args: vec![
                        col("Y"),
                        col("M"),
                        col("d"),
                        col("h"),
                        col("min"),
                        col("s"),
                        col("ms"),
                        lit("UTC"),
                    ],
                }
                .alias("ts_utc"),
                Expr::ScalarUDF {
                    fun: Arc::new(MAKE_UTC_TIMESTAMP.clone()),
                    args: vec![
                        col("Y"),
                        col("M"),
                        col("d"),
                        col("h"),
                        col("min"),
                        col("s"),
                        col("ms"),
                        lit("America/New_York"),
                    ],
                }
                .alias("ts_nyc"),
                Expr::ScalarUDF {
                    fun: Arc::new(MAKE_UTC_TIMESTAMP.clone()),
                    args: vec![
                        col("Y"),
                        col("M"),
                        col("d"),
                        col("h"),
                        col("min"),
                        col("s"),
                        col("ms"),
                        lit("America/Los_Angeles"),
                    ],
                }
                .alias("ts_la"),
            ])
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
            "select",
            "test_make_timestamp_tz",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
