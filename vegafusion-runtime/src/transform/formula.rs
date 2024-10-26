use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::Formula;

use crate::expression::compiler::utils::VfSimplifyInfo;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use vegafusion_common::column::flat_col;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Formula {
    async fn eval(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let formula_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;

        // Simplify expression prior to evaluation
        let simplifier = ExprSimplifier::new(VfSimplifyInfo::from(dataframe.schema().clone()));
        let formula_expr = simplifier.simplify(formula_expr)?;

        // Rename with alias
        let formula_expr = formula_expr.alias(&self.r#as);

        // Build selections. If as field name is already present, replace it with
        // formula expression at the same position. Otherwise append formula expression
        // to the end of the selection list
        let mut selections = Vec::new();
        let mut as_field_added = false;
        for field in dataframe.schema().fields().iter() {
            if field.name() == &self.r#as {
                selections.push(formula_expr.clone());
                as_field_added = true;
            } else {
                selections.push(flat_col(field.name()))
            }
        }
        if !as_field_added {
            selections.push(formula_expr);
        }

        // dataframe
        let result = dataframe.select(selections).with_context(|| {
            format!(
                "Formula transform failed with expression: {}",
                &self.expr.as_ref().unwrap()
            )
        })?;

        Ok((result, Default::default()))
    }
}
