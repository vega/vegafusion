use std::sync::Arc;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::ScalarUDF;
use vegafusion_datafusion_udfs::udafs::{Q1_UDF, Q3_UDF};
use vegafusion_datafusion_udfs::udfs::datetime::make_timestamptz::MAKE_UTC_TIMESTAMP;
use vegafusion_datafusion_udfs::udfs::datetime::timeunit::TIMEUNIT_START_UDF;


pub fn make_datafusion_context() -> SessionContext {
    let mut config = SessionConfig::new();
    let options = config.options_mut();
    options.optimizer.skip_failed_rules = true;
    let runtime = Arc::new(RuntimeEnv::default());
    let session_state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(session_state);

    // datetime
    ctx.register_udf((*MAKE_UTC_TIMESTAMP).clone());

    // timeunit
    ctx.register_udf((*TIMEUNIT_START_UDF).clone());

    // q1/q3 aggregate functions
    ctx.register_udaf((*Q1_UDF).clone());
    ctx.register_udaf((*Q3_UDF).clone());

    ctx
}