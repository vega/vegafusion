use std::sync::Arc;
use datafusion::arrow::datatypes::{Field, Schema};
use sqlx::SqlitePool;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
use sqlgen::ast::{Query, SetExpr, TableFactor, TableWithJoins, Ident, ObjectName, Select, SelectItem};
use sqlgen::dialect::DialectDisplay;
use sqlgen::parser::Parser;
use vegafusion_rt_datafusion::sql::connection::SqlConnection;
use vegafusion_rt_datafusion::sql::connection::sqlite_conn::SqLiteConnection;

mod util;
use util::crate_dir;


#[tokio::test]
async fn try_it() {
    let conn = SqLiteConnection::try_new(
        &format!("{}/tests/data/vega_datasets.db", crate_dir())
    ).await.unwrap();

    let schema = Schema::new(vec![
        // Field::new("index", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, true),
        // Field::new("date", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]);

    // let query = Query::select_star_from("stock");
    let query = Parser::parse_sql_query("SELECT symbol, price from stock").unwrap();
    let query_str = query.sql(&conn.dialect()).unwrap();

    let result = conn.fetch_query(&query_str, &schema).await.unwrap();

    println!("{}", result.pretty_format(Some(10)).unwrap());
}
