#[macro_use]
extern crate lazy_static;

mod utils;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, expr, Expr};
use rstest::rstest;
use rstest_reuse::{self, *};
use serde_json::json;
use std::sync::Arc;
use utils::{check_dataframe_query, dialect_names, make_connection, TOKIO_RUNTIME};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_dataframe::dataframe::DataFrame;
use vegafusion_sql::connection::SqlConnection;
use vegafusion_sql::dataframe::SqlDataFrame;

fn impute_data(conn: Arc<dyn SqlConnection>, ordering: bool) -> Arc<dyn DataFrame> {
    let table = if ordering {
        VegaFusionTable::from_json(
            &json!([
                {"_order": 1, "a": 0, "b": 28, "c": 0, "d": -1},
                {"_order": 2, "a": 0, "b": 91, "c": 1, "d": -1},
                {"_order": 3, "a": 1, "b": 43, "c": 0, "d": -2},
                {"_order": 4, "a": null, "b": 55, "c": 1, "d": -2},
                {"_order": 5, "a": 3, "b": 19, "c": 0, "d": -3},
                {"_order": 6, "a": 2, "b": 81, "c": 0, "d": -3},
                {"_order": 7, "a": 2, "b": 53, "c": 1, "d": -4},
            ]),
            1024,
        )
        .unwrap()
    } else {
        VegaFusionTable::from_json(
            &json!([
                {"a": 0, "b": 28, "c": 0, "d": -1},
                {"a": 0, "b": 91, "c": 1, "d": -1},
                {"a": 1, "b": 43, "c": 0, "d": -2},
                {"a": null, "b": 55, "c": 1, "d": -2},
                {"a": 3, "b": 19, "c": 0, "d": -3},
                {"a": 2, "b": 81, "c": 0, "d": -3},
                {"a": 2, "b": 53, "c": 1, "d": -4},

            ]),
            1024,
        )
        .unwrap()
    };

    SqlDataFrame::from_values(&table, conn).unwrap()
}

#[cfg(test)]
mod test_unordered_no_groups {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = impute_data(conn, false);

        let df_result = df
            .impute("a", ScalarValue::from(-1), "b", &[], None)
            .and_then(|df| {
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
            });

        check_dataframe_query(
            df_result,
            "impute",
            "unordered_no_groups",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_unordered_one_group {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = impute_data(conn, false);

        let df_result = df
            .impute("b", ScalarValue::from(-1), "a", &["c".to_string()], None)
            .and_then(|df| {
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
            });

        check_dataframe_query(
            df_result,
            "impute",
            "unordered_one_group",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_unordered_two_groups {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = impute_data(conn, false);

        let df_result = df
            .impute(
                "b",
                ScalarValue::from(-1),
                "a",
                &["c".to_string(), "d".to_string()],
                None,
            )
            .and_then(|df| {
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
                        Expr::Sort(expr::Sort {
                            expr: Box::new(col("c")),
                            asc: true,
                            nulls_first: true,
                        }),
                        Expr::Sort(expr::Sort {
                            expr: Box::new(col("d")),
                            asc: true,
                            nulls_first: true,
                        }),
                    ],
                    None,
                )
            });

        check_dataframe_query(
            df_result,
            "impute",
            "unordered_two_groups",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_ordered_no_groups {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = impute_data(conn, true);

        let df_result = df
            .impute("a", ScalarValue::from(-1), "b", &[], Some("_order"))
            .and_then(|df| {
                df.sort(
                    vec![Expr::Sort(expr::Sort {
                        expr: Box::new(col("_order")),
                        asc: true,
                        nulls_first: true,
                    })],
                    None,
                )
            });

        check_dataframe_query(
            df_result,
            "impute",
            "ordered_no_groups",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_ordered_one_group {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = impute_data(conn, true);

        let df_result = df
            .impute(
                "b",
                ScalarValue::from(-1),
                "a",
                &["c".to_string()],
                Some("_order"),
            )
            .and_then(|df| {
                df.sort(
                    vec![Expr::Sort(expr::Sort {
                        expr: Box::new(col("_order")),
                        asc: true,
                        nulls_first: true,
                    })],
                    None,
                )
            });

        check_dataframe_query(
            df_result,
            "impute",
            "ordered_one_group",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}

#[cfg(test)]
mod test_ordered_two_groups {
    use crate::*;

    #[apply(dialect_names)]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let df = impute_data(conn, true);

        let df_result = df
            .impute(
                "b",
                ScalarValue::from(-1),
                "a",
                &["c".to_string(), "d".to_string()],
                Some("_order"),
            )
            .and_then(|df| {
                df.sort(
                    vec![Expr::Sort(expr::Sort {
                        expr: Box::new(col("_order")),
                        asc: true,
                        nulls_first: true,
                    })],
                    None,
                )
            });

        check_dataframe_query(
            df_result,
            "impute",
            "ordered_two_groups",
            dialect_name,
            evaluable,
        );
    }

    #[test]
    fn test_marker() {} // Help IDE detect test module
}
