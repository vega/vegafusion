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

use crate::expression::compiler::utils::{cast_to, to_boolean, VfSimplifyInfo};
use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use std::sync::Arc;
use datafusion::logical_plan::ExprSimplifiable;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::transforms::Filter;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Filter {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        let filter_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(&dataframe.schema_df()),
        )?;

        // Cast filter expr to boolean
        let filter_expr = to_boolean(filter_expr, &dataframe.schema_df())?;

        // Simplify expression prior to evaluation
        let simplified_expr = filter_expr.simplify(&VfSimplifyInfo::from(dataframe.schema_df()))?;

        let result = dataframe.filter(simplified_expr)?;

        Ok((result, Default::default()))
    }
}
