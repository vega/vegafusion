use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use crate::expression::compiler::utils::ExprHelpers;
use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, Float64Array};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::arrow::datatypes::{Field, Schema, SchemaRef};
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::Result;
use vegafusion_core::proto::gen::transforms::Sequence;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Sequence {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let start_expr = compile(self.start.as_ref().unwrap(), config, None)?;
        let start_scalar = start_expr.eval_to_scalar()?;
        let start = start_scalar.to_f64()?;

        let stop_expr = compile(self.stop.as_ref().unwrap(), config, None)?;
        let stop_scalar = stop_expr.eval_to_scalar()?;
        let stop = stop_scalar.to_f64()?;

        // Use input DataFrame's connection to create the new dataset
        let conn = dataframe.connection();

        let step = if let Some(step_signal) = &self.step {
            let step_expr = compile(step_signal, config, None)?;
            let step_scalar = step_expr.eval_to_scalar()?;
            step_scalar.to_f64()?
        } else if stop >= start {
            1.0
        } else {
            -1.0
        };

        let capacity = ((stop - start).abs() / step.abs()).ceil() as usize;
        let mut data_builder = Float64Array::builder(capacity);
        let mut val = start;
        if start <= stop && step > 0.0 {
            while val < stop {
                data_builder.append_value(val);
                val += step;
            }
        } else if step < 0.0 {
            while val > stop {
                data_builder.append_value(val);
                val += step;
            }
        }
        let data_array = Arc::new(data_builder.finish()) as ArrayRef;
        let col_name = self.r#as.clone().unwrap_or_else(|| "data".to_string());
        let data_schema = Arc::new(Schema::new(vec![Field::new(
            &col_name,
            DataType::Float64,
            true,
        )])) as SchemaRef;
        let data_batch = RecordBatch::try_new(data_schema, vec![data_array])?;
        let data_table = VegaFusionTable::from(data_batch);
        let result = conn.scan_arrow(data_table.with_ordering()?).await?;

        Ok((result, Default::default()))
    }
}
