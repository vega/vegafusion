#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_expr::{expr, lit, round, Expr};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use std::ops::{Div, Mul};
use std::sync::Arc;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::column::flat_col;
use vegafusion_common::{data::table::VegaFusionTable, error::Result};
use vegafusion_dataframe::dataframe::{DataFrame, StackMode};
use vegafusion_sql::connection::SqlConnection;
use vegafusion_sql::dataframe::SqlDataFrame;

fn stack_data(conn: Arc<dyn SqlConnection>) -> Arc<dyn DataFrame> {
    let table = VegaFusionTable::from_json(&json!([
        {"a": 1, "b": 9, "c": "A"},
        {"a": -3, "b": 8, "c": "BB"},
        {"a": 5, "b": 7, "c": "A"},
        {"a": -7, "b": 6, "c": "BB"},
        {"a": 9, "b": 5, "c": "BB"},
        {"a": -11, "b": 4, "c": "A"},
        {"a": 13, "b": 3, "c": "BB"},
    ]))
    .unwrap();

    SqlDataFrame::from_values(&table, conn, Default::default()).unwrap()
}

async fn make_stack_for_mode(
    df: Arc<dyn DataFrame>,
    mode: StackMode,
) -> Result<Arc<dyn DataFrame>> {
    df.stack(
        "a",
        vec![Expr::Sort(expr::Sort {
            expr: Box::new(flat_col("b")),
            asc: true,
            nulls_first: true,
        })],
        &["c".to_string()],
        "start",
        "end",
        mode,
    )
    .await
    .unwrap()
    .sort(
        vec![
            Expr::Sort(expr::Sort {
                expr: Box::new(flat_col("c")),
                asc: true,
                nulls_first: true,
            }),
            Expr::Sort(expr::Sort {
                expr: Box::new(flat_col("end")),
                asc: true,
                nulls_first: true,
            }),
        ],
        None,
    )
    .await
}

#[cfg(test)]
mod test_mode_zero {
    use crate::*;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = stack_data(conn);
        let df_result = make_stack_for_mode(df, StackMode::Zero).await;
        check_dataframe_query(df_result, "stack", "mode_zero", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_mode_center {
    use crate::*;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));
        let df = stack_data(conn);
        let df_result = make_stack_for_mode(df, StackMode::Center).await;
        check_dataframe_query(df_result, "stack", "mode_center", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_mode_normalized {
    use crate::*;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));
        let df = stack_data(conn);
        let df = make_stack_for_mode(df, StackMode::Normalize).await.unwrap();
        // Round start and end to 2 decimal places to avoid numerical precision issues when comparing results
        let df_result = df
            .select(vec![
                flat_col("a"),
                flat_col("b"),
                flat_col("c"),
                round(vec![flat_col("start").mul(lit(100))])
                    .div(lit(100))
                    .alias("trunc_start"),
                round(vec![flat_col("end").mul(lit(100))])
                    .div(lit(100))
                    .alias("trunc_end"),
            ])
            .await;
        check_dataframe_query(
            df_result,
            "stack",
            "mode_normalized",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
