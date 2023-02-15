#[macro_use]
extern crate lazy_static;

mod utils;
use utils::{TOKIO_RUNTIME, make_connection, check_dataframe_query};
use datafusion_expr::{col, expr, Expr};
use rstest::rstest;
use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_sql::dataframe::SqlDataFrame;


#[cfg(test)]
mod test_simple_fold {
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
              {"country": "USA", "gold": 10, "silver": 20},
              {"country": "Canada", "gold": 7, "silver": 26}
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
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
            .and_then(|df| {
                df.sort(
                    vec![
                        Expr::Sort(expr::Sort {
                            expr: Box::new(col("country")),
                            asc: true,
                            nulls_first: true,
                        }),
                        Expr::Sort(expr::Sort {
                            expr: Box::new(col("key")),
                            asc: true,
                            nulls_first: true,
                        }),
                    ],
                    None,
                )
            });

        check_dataframe_query(df_result, "fold", "simple_fold", dialect_name, evaluable);
    }
}


#[cfg(test)]
mod test_ordered_fold {
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
              {"_order": 1, "country": "USA", "gold": 10, "silver": 20},
              {"_order": 2, "country": "Canada", "gold": 7, "silver": 26}
            ]),
            1024,
        )
            .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
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

        check_dataframe_query(df_result, "fold", "ordered_fold", dialect_name, evaluable);
    }
}
