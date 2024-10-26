use crate::datafusion::context::make_datafusion_context;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{ExprSchema, ScalarValue};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{Expr, ExprSchemable, TryCast};
use datafusion_optimizer::simplify_expressions::SimplifyInfo;
use datafusion_physical_expr::execution_props::ExecutionProps;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, BooleanArray};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::datafusion_common::{Column, DFSchema, DataFusionError};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

lazy_static! {
    pub static ref UNIT_RECORD_BATCH: RecordBatch = RecordBatch::try_from_iter(vec![(
        "__unit__",
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef
    )])
    .unwrap();
    pub static ref UNIT_SCHEMA: DFSchema =
        DFSchema::try_from(UNIT_RECORD_BATCH.schema().as_ref().clone()).unwrap();
}

pub trait ExprHelpers {
    fn columns(&self) -> Result<HashSet<Column>>;
    fn to_phys_expr(&self) -> Result<Arc<dyn PhysicalExpr>>;
    fn eval_to_scalar(&self) -> Result<ScalarValue>;
    fn try_cast_to(
        self,
        cast_to_type: &DataType,
        schema: &dyn ExprSchema,
    ) -> datafusion_common::Result<Expr>;
}

impl ExprHelpers for Expr {
    fn columns(&self) -> Result<HashSet<Column>> {
        let mut columns: HashSet<Column> = HashSet::new();
        expr_to_columns(self, &mut columns)
            .with_context(|| format!("Failed to collect columns from expression: {self:?}"))?;
        Ok(columns)
    }

    fn to_phys_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        let ctx = make_datafusion_context();
        let phys_expr = ctx.create_physical_expr(self.clone(), &UNIT_SCHEMA)?;
        Ok(phys_expr)
    }

    fn eval_to_scalar(&self) -> Result<ScalarValue> {
        if !self.columns()?.is_empty() {
            return Err(VegaFusionError::compilation(format!(
                "Cannot eval_to_scalar for Expr with column references: {self:?}"
            )));
        }

        let phys_expr = self.to_phys_expr()?;
        let col_result = phys_expr.evaluate(&UNIT_RECORD_BATCH)?;
        match col_result {
            ColumnarValue::Scalar(scalar) => Ok(scalar),
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return Err(VegaFusionError::compilation(format!(
                        "Unexpected non-scalar array result when evaluate expr: {self:?}"
                    )));
                }
                ScalarValue::try_from_array(&array, 0).with_context(|| {
                    format!(
                        "Failed to convert scalar array result to ScalarValue in expr: {self:?}"
                    )
                })
            }
        }
    }

    fn try_cast_to(
        self,
        cast_to_type: &DataType,
        schema: &dyn ExprSchema,
    ) -> datafusion_common::Result<Expr> {
        // Based on cast_to, using TryCast instead of Cast
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }
        Ok(Expr::TryCast(TryCast::new(
            Box::new(self),
            cast_to_type.clone(),
        )))
    }
}

/// In order to simplify expressions, DataFusion must have information
/// about the expressions.
///
/// You can provide that information using DataFusion [DFSchema]
/// objects or from some other implemention
pub struct VfSimplifyInfo {
    /// The input schema
    schema: DFSchema,

    /// Execution specific details needed for constant evaluation such
    /// as the current time for `now()` and [VariableProviders]
    execution_props: ExecutionProps,
}

impl SimplifyInfo for VfSimplifyInfo {
    fn is_boolean_type(&self, expr: &Expr) -> std::result::Result<bool, DataFusionError> {
        Ok(matches!(expr.get_type(&self.schema)?, DataType::Boolean))
    }

    fn nullable(&self, expr: &Expr) -> std::result::Result<bool, DataFusionError> {
        expr.nullable(&self.schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    fn get_data_type(&self, expr: &Expr) -> std::result::Result<DataType, DataFusionError> {
        expr.get_type(&self.schema)
    }
}

impl From<DFSchema> for VfSimplifyInfo {
    fn from(schema: DFSchema) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }
}
