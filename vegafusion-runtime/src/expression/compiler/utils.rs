use datafusion_common::{ExprSchema, plan_err, ScalarValue};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{Cast, Expr, ExprSchemable, TryCast};
use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyInfo};
use datafusion_physical_expr::execution_props::ExecutionProps;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;
use datafusion_expr::expr_schema::cast_subquery;
use vegafusion_common::arrow::array::{ArrayRef, BooleanArray};
use vegafusion_common::arrow::compute::can_cast_types;
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
    // pub static ref SESSION_STATE: SessionState = default_session_builder(Default::default());
    // pub static ref PLANNER: DefaultPhysicalPlanner = Default::default();
}

pub trait ExprHelpers {
    fn columns(&self) -> Result<HashSet<Column>>;
    fn eval_to_scalar(&self) -> Result<ScalarValue>;
    fn try_cast_to(self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> datafusion_common::Result<Expr>;
}

impl ExprHelpers for Expr {
    fn columns(&self) -> Result<HashSet<Column>> {
        let mut columns: HashSet<Column> = HashSet::new();
        expr_to_columns(self, &mut columns)
            .with_context(|| format!("Failed to collect columns from expression: {self:?}"))?;
        Ok(columns)
    }

    fn eval_to_scalar(&self) -> Result<ScalarValue> {
        let simplifier = ExprSimplifier::new(VfSimplifyInfo::from(UNIT_SCHEMA.deref().clone()));
        let simplified_expr = simplifier.simplify(self.clone())?;
        if let Expr::Literal(scalar) = simplified_expr {
            Ok(scalar)
        } else {
            Err(VegaFusionError::internal(format!(
                "Failed to evaluate expression to scalar value: {self}\nsimplified to: {simplified_expr}\n"
            )))
        }
    }

    fn try_cast_to(self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> datafusion_common::Result<Expr> {
        // Based on cast_to, using TryCast instead of Cast
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }
        Ok(Expr::TryCast(TryCast::new(Box::new(self), cast_to_type.clone())))
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
