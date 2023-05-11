#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_expr::{expr, Expr};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_dataframe::dataframe::DataFrame;
use vegafusion_sql::dataframe::SqlDataFrame;

#[cfg(test)]
mod test_default_null_ordering {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 4, "c": "BB"},
            {"a": 2, "b": 6, "c": "DDDD"},
            {"a": null, "b": 5, "c": "BB"},
            {"a": 2, "b": 7, "c": "CCC"},
            {"a": 1, "b": 8, "c": "CCC"},
            {"a": 1, "b": 2, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let df = df.as_any().downcast_ref::<SqlDataFrame>().unwrap();

        let df_result = df
            .sort(
                vec![
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("a")),
                        asc: false,
                        nulls_first: false,
                    }),
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("c")),
                        asc: true,
                        nulls_first: true,
                    }),
                ],
                None,
            )
            .await;

        check_dataframe_query(
            df_result,
            "sort",
            "default_null_ordering",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_custom_null_ordering {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 4, "c": "BB"},
            {"a": 2, "b": 6, "c": "DDDD"},
            {"a": null, "b": 5, "c": "BB"},
            {"a": 2, "b": 7, "c": "CCC"},
            {"a": 1, "b": 8, "c": null},
            {"a": 1, "b": 2, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();
        let sort_res = df
            .sort(
                vec![
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("a")),
                        asc: false,
                        nulls_first: true,
                    }),
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("c")),
                        asc: true,
                        nulls_first: false,
                    }),
                ],
                None,
            )
            .await;

        check_dataframe_query(
            sort_res,
            "sort",
            "custom_null_ordering",
            dialect_name,
            evaluable,
        );
    }
}

#[cfg(test)]
mod test_order_with_limit {
    use crate::*;
    use vegafusion_common::column::flat_col;

    #[apply(dialect_names)]
    async fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(&json!([
            {"a": 1, "b": 4, "c": "BB"},
            {"a": 2, "b": 6, "c": "DDDD"},
            {"a": null, "b": 5, "c": "BB"},
            {"a": 4, "b": 7, "c": "CCC"},
            {"a": 5, "b": 8, "c": "CCC"},
            {"a": 6, "b": 2, "c": "A"},
        ]))
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn, Default::default()).unwrap();

        let df_result = df
            .sort(
                vec![
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("c")),
                        asc: true,
                        nulls_first: true,
                    }),
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col("b")),
                        asc: true,
                        nulls_first: true,
                    }),
                ],
                Some(4),
            )
            .await;

        check_dataframe_query(
            df_result,
            "sort",
            "order_with_limit",
            dialect_name,
            evaluable,
        );
    }
}
