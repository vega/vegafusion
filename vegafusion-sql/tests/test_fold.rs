#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_expr::{expr, Expr};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_simple_fold {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
          {"country": "USA", "gold": 10, "silver": 20},
          {"country": "Canada", "gold": 7, "silver": 26}
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .fold(
                &[
                    "gold".to_string(),
                    "silver".to_string(),
                    "bogus".to_string(),
                ],
                "value",
                "key",
                None,
            )
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("country")),
                        asc: true,
                        nulls_first: true,
                    }),
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("key")),
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

        check_dataframe_query(df_result, "fold", "simple_fold", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_ordered_fold {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
          {"_order": 1, "country": "USA", "gold": 10, "silver": 20},
          {"_order": 2, "country": "Canada", "gold": 7, "silver": 26}
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df_result = df
            .fold(
                &[
                    "gold".to_string(),
                    "silver".to_string(),
                    "bogus".to_string(),
                ],
                "value",
                "key",
                Some("_order"),
            )
            .await;

        let df_result = if let Ok(df) = df_result {
            df.sort(
                vec![Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col("_order")),
                    asc: true,
                    nulls_first: true,
                })],
                None,
            )
            .await
        } else {
            df_result
        };

        check_dataframe_query(df_result, "fold", "ordered_fold", dialect_name, evaluable);
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
