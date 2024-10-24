use std::sync::Arc;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::ScalarUDF;
use vegafusion_datafusion_udfs::udafs::{Q1_UDF, Q3_UDF};
use vegafusion_datafusion_udfs::udfs::array::indexof::IndexOfUDF;
use vegafusion_datafusion_udfs::udfs::datetime::epoch_to_utc_timestamp::EPOCH_MS_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::format_timestamp::FORMAT_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::from_utc_timestamp::FROM_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::make_timestamptz::MAKE_UTC_TIMESTAMP;
use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::STR_TO_UTC_TIMESTAMP_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::timeunit::TIMEUNIT_START_UDF;
use vegafusion_datafusion_udfs::udfs::datetime::utc_timestamp_to_epoch::UTC_TIMESTAMP_TO_EPOCH_MS;
use vegafusion_datafusion_udfs::udfs::datetime::utc_timestamp_to_str::UTC_TIMESTAMP_TO_STR_UDF;
use vegafusion_datafusion_udfs::udfs::math::isfinite::IsFiniteUDF;


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

    // isFinite
    ctx.register_udf(ScalarUDF::from(IsFiniteUDF::new()));

    // datetime
    ctx.register_udf((*UTC_TIMESTAMP_TO_STR_UDF).clone());
    ctx.register_udf((*FROM_UTC_TIMESTAMP_UDF).clone());
    ctx.register_udf((*EPOCH_MS_TO_UTC_TIMESTAMP_UDF).clone());
    ctx.register_udf((*STR_TO_UTC_TIMESTAMP_UDF).clone());
    ctx.register_udf((*MAKE_UTC_TIMESTAMP).clone());
    ctx.register_udf((*UTC_TIMESTAMP_TO_EPOCH_MS).clone());

    // timeunit
    ctx.register_udf((*TIMEUNIT_START_UDF).clone());

    // timeformat
    ctx.register_udf((*FORMAT_TIMESTAMP_UDF).clone());

    // list
    ctx.register_udf(ScalarUDF::from(IndexOfUDF::new()));

    // q1/q3 aggregate functions
    ctx.register_udaf((*Q1_UDF).clone());
    ctx.register_udaf((*Q3_UDF).clone());

    ctx
}