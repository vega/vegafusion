/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;

use crate::expression::compiler::utils::cast_to;
use async_trait::async_trait;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Filter;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Filter {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        let logical_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;
        // Save off initial columns and select them below to filter out any intermediary columns
        // that the expression may produce
        let col_names: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let result = dataframe
            .filter(cast_to(
                logical_expr,
                &DataType::Boolean,
                dataframe.schema(),
            )?)?
            .select_columns(&col_names)?;

        Ok((result, Default::default()))
    }
}
