use std::sync::Arc;
use vegafusion_core::proto::gen::transforms::{TimeUnit, TimeUnitUnit};
use vegafusion_core::error::{Result, VegaFusionError};
use crate::transform::TransformTrait;
use datafusion::prelude::{DataFrame, col};
use crate::expression::compiler::config::CompilationConfig;
use vegafusion_core::task_graph::task_value::TaskValue;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Date64Array, Int64Array, TimestampNanosecondArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::TimeUnit as ArrowTimeUnit;
use datafusion::arrow::compute::kernels::arity::unary;
use datafusion::arrow::temporal_conversions::{date64_to_datetime, timestamp_ns_to_datetime};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::functions::{make_scalar_function, ReturnTypeFunction, Signature, Volatility};
use datafusion::physical_plan::udf::ScalarUDF;
use chrono::{Datelike, Timelike};
use crate::expression::compiler::utils::{cast_to, UNIT_SCHEMA};

#[async_trait]
impl TransformTrait for TimeUnit {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {

        let units: Vec<_> = self.units.clone().into_iter().map(|unit| TimeUnitUnit::from_i32(unit).unwrap()).collect();

        let timeunit = move |args: &[ArrayRef]| {
            let arg = &args[0];

            let array = arg
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap();

            use TimeUnitUnit::*;
            let result_array: Int64Array = match units.as_slice() {
                &[Year] => {
                    unary(array, |value| {
                        let dt_value = date64_to_datetime(value).with_nanosecond(0).unwrap();
                        let dt_value = dt_value
                            .with_second(0).unwrap()
                            .with_minute(0).unwrap()
                            .with_hour(0).unwrap()
                            .with_day0(0).unwrap()
                            .with_month0(0).unwrap();
                        // dt_value.timestamp_nanos()
                        dt_value.timestamp_millis()
                    })
                }
                _ => {
                    return Err(DataFusionError::Internal(format!("Unsupported timeunit units: {:?}", units)))
                }
            };

            Ok(Arc::new(result_array) as ArrayRef)
        };

        let timeunit = make_scalar_function(timeunit);
        let return_type: ReturnTypeFunction = Arc::new(
            // move |_| Ok(Arc::new(DataType::Timestamp(ArrowTimeUnit::Nanosecond, None)))
            // move |_| Ok(Arc::new(DataType::Date64))
            move |_| Ok(Arc::new(DataType::Int64))
        );

        let timeunit_udf = ScalarUDF::new(
            "timeunit",
            &Signature::uniform(
                1,
                vec![DataType::Date64],
                Volatility::Immutable,
            ),
            &return_type,
            &timeunit,
        );

        let timeunit_value = timeunit_udf.call(vec![
            cast_to(col(&self.field), &DataType::Date64, dataframe.schema()).unwrap()
        ]);

        // Apply alias
        let timeunit_value = if let Some(alias_0) = &self.alias_0 {
            timeunit_value.alias(alias_0)
        } else {
            timeunit_value.alias("unit0")
        };

        let dataframe = dataframe
            .select(vec![Expr::Wildcard, timeunit_value])?;

        Ok((dataframe.clone(), Vec::new()))
    }
}

// fn timeunit_single(unit: &[TimeUnitUnit], value: i64) -> Result<i64> {
//     use TimeUnitUnit::*;
//     let result_value = match unit {
//         &[Year] => {}
//         _ => {
//             return Err()
//         }
//     };
//
//     todo!()
// }
