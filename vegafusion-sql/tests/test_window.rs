#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    col, expr, lit, BuiltInWindowFunction, Expr, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_simple_aggs_unbounded {
    use crate::*;
    use datafusion_expr::WindowFunctionDefinition;
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 9, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: flat_col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new(Some(true));
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(avg_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(min_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(max_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("max_b"),
            ])
            .await;
        let df_result = if let Ok(df) = df_result {
            df.sort(order_by, None).await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "simple_aggs_unbounded",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_simple_aggs_unbounded_groups {
    use crate::*;
    use datafusion_expr::test::function_stub::avg_udaf;
    use datafusion_expr::WindowFunctionDefinition;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 9, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: flat_col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Preceding(ScalarValue::Null),
            WindowFrameBound::CurrentRow,
        );
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(avg_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(min_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(max_udaf()),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("max_b"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(order_by, None).await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "simple_aggs_unbounded_groups",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_simple_aggs_bounded {
    use crate::*;
    use datafusion_expr::WindowFunctionDefinition;
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 9, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: flat_col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::from(1)),
            WindowFrameBound::CurrentRow,
        );
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(avg_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(min_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(max_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("max_b"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(order_by, None).await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "simple_aggs_bounded",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_simple_aggs_bounded_groups {
    use crate::*;
    use datafusion_expr::WindowFunctionDefinition;
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 1, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 7, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Preceding(ScalarValue::from(1)),
            WindowFrameBound::CurrentRow,
        );
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(avg_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(min_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::AggregateUDF(max_udaf()),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("max_b"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![
                    expr::Sort {
                        expr: col("a"),
                        asc: true,
                        nulls_first: true,
                    },
                    expr::Sort {
                        expr: col("b"),
                        asc: true,
                        nulls_first: true,
                    },
                ],
                None,
            )
            .await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "simple_aggs_bounded_groups",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_simple_window_fns {
    use crate::*;
    use datafusion_expr::WindowFunctionDefinition;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use std::sync::Arc;

        use datafusion::functions_window::row_number::RowNumber;
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 9, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new(Some(true));
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(
                        Arc::new(RowNumber::new().into()),
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("row_num"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::Rank,
                    ),
                    args: vec![],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("rank"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::DenseRank,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("d_rank"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::FirstValue,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("first"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::LastValue,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("last"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(order_by, None).await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "simple_window_fns",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_advanced_window_fns {
    use crate::*;
    use datafusion_expr::WindowFunctionDefinition;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 9, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new(Some(true));
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::NthValue,
                    ),
                    args: vec![col("b"), lit(1)],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("nth1"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::CumeDist,
                    ),
                    args: vec![],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("cdist"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::Lag,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("lag_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::Lead,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("lead_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::BuiltInWindowFunction(
                        BuiltInWindowFunction::Ntile,
                    ),
                    args: vec![lit(2)],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("ntile"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(order_by, None).await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "advanced_window_fns",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_unordered_row_number {
    use crate::*;
    use datafusion_expr::WindowFunctionDefinition;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        use std::sync::Arc;

        use datafusion::functions_window::row_number::RowNumber;
        use sqlparser::ast::NullTreatment;

        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 2, "c": "A"},
            {"a": 3, "b": 4, "c": "BB"},
            {"a": 5, "b": 6, "c": "A"},
            {"a": 7, "b": 8, "c": "BB"},
            {"a": 9, "b": 10, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let order_by = vec![expr::Sort {
            expr: col("a"),
            asc: true,
            nulls_first: true,
        }];
        let window_frame = WindowFrame::new(Some(true));
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(
                        Arc::new(RowNumber::new().into()),
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: window_frame.clone(),
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias("row_num"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(order_by, None).await
        } else {
            df_result
        };

        check_dataframe_query(
            df_result,
            "select_window",
            "row_number_no_order",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
