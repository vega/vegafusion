#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use vegafusion_sql::connection::{DummySqlConnection, SqlConnection};
use vegafusion_sql::dialect::Dialect;

#[cfg(feature = "datafusion-conn")]
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

#[cfg(feature = "sqlite-conn")]
use vegafusion_sql::connection::sqlite_conn::SqLiteConnection;

lazy_static! {
    pub static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

#[cfg(test)]
mod test_values {
    use crate::*;
    use rstest::rstest;
    use serde_json::json;
    use vegafusion_common::data::table::VegaFusionTable;
    use vegafusion_dataframe::dataframe::DataFrame;
    use vegafusion_sql::dataframe::SqlDataFrame;

    #[rstest(
        dialect_name,
        case("athena"),
        case("bigquery"),
        case("clickhouse"),
        case("databricks"),
        case("datafusion"),
        case("dremio"),
        case("duckdb"),
        case("generic"),
        case("mysql"),
        case("postgres"),
        case("redshift"),
        case("snowflake"),
        case("sqlite")
    )]
    fn test(dialect_name: &str) {
        println!("{dialect_name}");
        let (expected_query, expected_table) =
            load_expected_query_and_result("values", "values1", dialect_name);

        let (conn, evaluable) = TOKIO_RUNTIME.block_on(make_connection(dialect_name));

        let table = VegaFusionTable::from_json(
            &json!([
                {"a": 1, "b": 2, "c": "A"},
                {"a": 3, "b": 4, "c": "BB"},
                {"a": 5, "b": 6, "c": "CCC"},
            ]),
            1024,
        )
        .unwrap();

        let df = SqlDataFrame::from_values(&table, conn).unwrap();
        let df = df.as_any().downcast_ref::<SqlDataFrame>().unwrap();

        let sql = df.as_query().to_string();
        println!("{sql}");
        assert_eq!(sql, expected_query);

        if evaluable {
            let table: VegaFusionTable = TOKIO_RUNTIME.block_on(df.collect()).unwrap();
            let table_str = table.pretty_format(None).unwrap();
            println!("{table_str}");
            assert_eq!(table_str, expected_table);
        }
    }
}

async fn make_connection(name: &str) -> (Arc<dyn SqlConnection>, bool) {
    #[cfg(feature = "datafusion-conn")]
    if name == "datafusion" {
        return (Arc::new(DataFusionConnection::default()), true);
    }

    #[cfg(feature = "sqlite-conn")]
    if name == "sqlite" {
        let conn = SqLiteConnection::try_new("file::memory:?cache=shared")
            .await
            .unwrap();
        return (Arc::new(conn), true);
    }

    let dialect = Dialect::from_str(name).unwrap();
    (Arc::new(DummySqlConnection::new(dialect)), false)
}

// Utilities
fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

fn load_expected_toml(name: &str) -> HashMap<String, HashMap<String, String>> {
    // Load spec
    let toml_path = format!("{}/tests/expected/{}.toml", crate_dir(), name);
    let toml_str = fs::read_to_string(toml_path).unwrap();
    toml::from_str(&toml_str).unwrap()
}

fn load_expected_query_and_result(
    suite_name: &str,
    test_name: &str,
    dialect_name: &str,
) -> (String, String) {
    let expected = load_expected_toml(suite_name);
    let expected = expected.get(test_name).unwrap();
    let expected_query = expected.get(dialect_name).unwrap().trim();
    let expected_table = expected.get("result").unwrap().trim();
    (expected_query.to_string(), expected_table.to_string())
}
