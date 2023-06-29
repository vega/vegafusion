#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    col, expr, lit, window_function, AggregateFunction, BuiltInWindowFunction, Expr, WindowFrame,
    WindowFrameBound, WindowFrameUnits,
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
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(flat_col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Sum),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(
                        AggregateFunction::Count,
                    ),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Avg),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Min),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Max),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
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
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(flat_col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Groups,
            start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Sum),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(
                        AggregateFunction::Count,
                    ),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Avg),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Min),
                    args: vec![flat_col("b")],
                    partition_by: vec![flat_col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Max),
                    args: vec![flat_col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
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
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(flat_col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::from(1)),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Sum),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(
                        AggregateFunction::Count,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Avg),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Min),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Max),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
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

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Groups,
            start_bound: WindowFrameBound::Preceding(ScalarValue::from(1)),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Sum),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("sum_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(
                        AggregateFunction::Count,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("count_part_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Avg),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("cume_mean_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Min),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("min_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::AggregateFunction(AggregateFunction::Max),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
                })
                .alias("max_b"),
            ])
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![
                    Expr::Sort(expr::Sort {
                        expr: Box::new(col("a")),
                        asc: true,
                        nulls_first: true,
                    }),
                    Expr::Sort(expr::Sort {
                        expr: Box::new(col("b")),
                        asc: true,
                        nulls_first: true,
                    }),
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

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::RowNumber,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("row_num"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::Rank,
                    ),
                    args: vec![],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("rank"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::DenseRank,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("d_rank"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::FirstValue,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("first"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::LastValue,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
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

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::NthValue,
                    ),
                    args: vec![col("b"), lit(1)],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("nth1"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::CumeDist,
                    ),
                    args: vec![],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("cdist"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::Lag,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("lag_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::Lead,
                    ),
                    args: vec![col("b")],
                    partition_by: vec![col("c")],
                    order_by: order_by.clone(),
                    window_frame: window_frame.clone(),
                })
                .alias("lead_b"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::Ntile,
                    ),
                    args: vec![lit(2)],
                    partition_by: vec![],
                    order_by: order_by.clone(),
                    window_frame,
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

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
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
        let order_by = vec![Expr::Sort(expr::Sort {
            expr: Box::new(col("a")),
            asc: true,
            nulls_first: true,
        })];
        let window_frame = WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::Null),
            end_bound: WindowFrameBound::CurrentRow,
        };
        let df_result = df
            .select(vec![
                col("a"),
                col("b"),
                col("c"),
                Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::RowNumber,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: vec![],
                    window_frame: window_frame.clone(),
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
