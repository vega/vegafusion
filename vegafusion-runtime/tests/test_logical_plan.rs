use datafusion::prelude::SessionContext;
use datafusion_expr::lit;
use serde_json::json;
use vegafusion_common::data::table::VegaFusionTable;

#[tokio::test]
async fn test_is_batch_in_plan() {
    let tbl = VegaFusionTable::from_json(&json!([
        {"a": 1, "b": "A"},
        {"a": 2, "b": "BB"},
        {"a": 3, "b": "CCC"},
    ]))
    .unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batches(tbl.batches).unwrap();
    df.clone().show().await.unwrap();

    // let plan = df.into_unoptimized_plan();
    // println!("{:#?}", plan);

    let (_session_state, _plan) = df.into_parts();
    // session_state.register_udf()

    let foo = lit(2) + lit("a") + lit(4);
    println!("foo: {:?}", foo);

    // println!("it works")
}
