use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::Runtime;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_dataframe::dataframe::DataFrame;
use vegafusion_sql::connection::{DummySqlConnection, SqlConnection};
use vegafusion_sql::dialect::Dialect;

use rstest_reuse::{self, *};

#[cfg(feature = "datafusion-conn")]
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;
use vegafusion_sql::dataframe::SqlDataFrame;

lazy_static! {
    pub static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
}

#[template]
#[rstest]
#[case("athena")]
#[case("bigquery")]
#[case("clickhouse")]
#[case("databricks")]
#[case("datafusion")]
#[case("duckdb")]
#[case("mysql")]
#[case("postgres")]
#[case("redshift")]
#[case("snowflake")]
pub fn dialect_names(#[case] dialect_name: &str) {}

pub async fn make_connection(name: &str) -> (Arc<dyn SqlConnection>, bool) {
    #[cfg(feature = "datafusion-conn")]
    if name == "datafusion" {
        return (Arc::new(DataFusionConnection::default()), true);
    }

    let dialect = Dialect::from_str(name).unwrap();
    (Arc::new(DummySqlConnection::new(dialect)), false)
}

// Utilities
pub fn crate_dir() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .display()
        .to_string()
}

pub fn load_expected_toml(name: &str) -> HashMap<String, HashMap<String, String>> {
    // Load spec
    let toml_path = format!("{}/tests/expected/{}.toml", crate_dir(), name);
    let toml_str = fs::read_to_string(toml_path).unwrap();
    toml::from_str(&toml_str).unwrap()
}

pub fn load_expected_query_and_result(
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

pub fn check_dataframe_query(
    df_result: Result<Arc<dyn DataFrame>>,
    suite_name: &str,
    test_name: &str,
    dialect_name: &str,
    evaluable: bool,
) {
    let (expected_query, expected_table) =
        load_expected_query_and_result(suite_name, test_name, dialect_name);

    if expected_query == "UNSUPPORTED" {
        if let Err(VegaFusionError::SqlNotSupported(..)) = df_result {
            // expected, return successful
            println!("Unsupported");
            return;
        } else {
            panic!("Expected query result to be an error")
        }
    }
    let df = df_result.unwrap();

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
