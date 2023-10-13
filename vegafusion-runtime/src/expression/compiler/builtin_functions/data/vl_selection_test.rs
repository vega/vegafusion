use crate::expression::compiler::utils::ExprHelpers;

use std::collections::HashMap;
use std::convert::TryFrom;

use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_expr::expr::Case;
use datafusion_expr::{ceil, expr, lit, Between, Expr, ExprSchemable};
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::{DataType, TimeUnit};
use vegafusion_common::column::flat_col;
use vegafusion_common::data::scalar::ScalarValue;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datatypes::{
    cast_to, is_float_datatype, is_integer_datatype, is_numeric_datatype, is_string_datatype,
};
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::expression::literal::Value;
use vegafusion_core::proto::gen::{
    expression::expression::Expr as ProtoExpr, expression::Expression, expression::Literal,
};
use vegafusion_datafusion_udfs::udfs::datetime::str_to_utc_timestamp::{
    parse_datetime, STR_TO_UTC_TIMESTAMP_UDF,
};
use vegafusion_datafusion_udfs::udfs::datetime::utc_timestamp_to_epoch::UTC_TIMESTAMP_TO_EPOCH_MS;

/// Op
#[derive(Debug, Clone)]
pub enum Op {
    Union,
    Intersect,
}

impl Op {
    pub fn valid(s: &str) -> bool {
        Self::from_str(s).is_ok()
    }
}

impl FromStr for Op {
    type Err = VegaFusionError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "union" => Self::Union,
            "intersect" => Self::Intersect,
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "Invalid vlSelection operation: {s}"
                )))
            }
        })
    }
}

impl TryFrom<ScalarValue> for Op {
    type Error = VegaFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Utf8(Some(op)) => Self::from_str(&op),
            _ => Err(VegaFusionError::internal(
                "Expected selection op to be a string",
            )),
        }
    }
}

/// Selection Type
#[derive(Debug, Clone)]
pub enum SelectionType {
    Enum,
    RangeInc,
    RangeExc,
    RangeLe,
    RangeRe,
}

impl FromStr for SelectionType {
    type Err = VegaFusionError;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "E" => Self::Enum,
            "R" => Self::RangeInc,
            "R-E" => Self::RangeExc,
            "R-LE" => Self::RangeLe,
            "R-RE" => Self::RangeRe,
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "Invalid selection type: {s}"
                )))
            }
        })
    }
}

impl TryFrom<ScalarValue> for SelectionType {
    type Error = VegaFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Utf8(Some(op)) => Self::from_str(&op),
            _ => Err(VegaFusionError::internal(
                "Expected selection type to be a string",
            )),
        }
    }
}

/// Field specification
#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub field: String,
    pub typ: SelectionType,
}

impl FieldSpec {
    pub fn to_test_expr(
        &self,
        values: &ScalarValue,
        schema: &DFSchema,
        default_input_tz: &str,
    ) -> Result<Expr> {
        let field_col = flat_col(&self.field);

        // Convert timestamp column to integer milliseconds before comparisons.
        let field_col = if matches!(
            field_col.get_type(schema)?,
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ) {
            Expr::ScalarUDF(expr::ScalarUDF {
                fun: Arc::new((*UTC_TIMESTAMP_TO_EPOCH_MS).clone()),
                args: vec![field_col],
            })
        } else {
            field_col
        };

        let expr = match self.typ {
            SelectionType::Enum => {
                let field_type = field_col.get_type(schema)?;
                let list_scalars = if let ScalarValue::List(Some(elements), _) = &values {
                    elements.clone()
                } else {
                    // convert values to single element list
                    vec![values.clone()]
                };

                let list_exprs = list_scalars
                    .into_iter()
                    .map(|scalar| {
                        Self::cast_test_scalar(scalar, &field_type, schema, default_input_tz)
                    })
                    .collect::<Result<Vec<_>>>()?;

                Expr::InList(expr::InList {
                    expr: Box::new(field_col),
                    list: list_exprs,
                    negated: false,
                })
            }
            _ => {
                let field_dtype = field_col
                    .get_type(schema)
                    .with_context(|| format!("Failed to infer type of column {}", self.field))?;

                // Cast string columns to float
                let (field_dtype, field_col) = if is_string_datatype(&field_dtype) {
                    (
                        DataType::Float64,
                        cast_to(field_col, &DataType::Float64, schema)?,
                    )
                } else {
                    (field_dtype, field_col)
                };

                let (low, high) = match &values {
                    ScalarValue::List(Some(elements), _) if elements.len() == 2 => {
                        let first = Self::cast_test_scalar(
                            elements[0].clone(),
                            &field_dtype,
                            schema,
                            default_input_tz,
                        )?;
                        let second = Self::cast_test_scalar(
                            elements[1].clone(),
                            &field_dtype,
                            schema,
                            default_input_tz,
                        )?;

                        // Don't assume elements are in ascending order
                        // Compute min and max values with Case expression
                        let low = Expr::Case(Case {
                            expr: None,
                            when_then_expr: vec![(
                                Box::new(first.clone().lt_eq(second.clone())),
                                Box::new(first.clone()),
                            )],
                            else_expr: Some(Box::new(second.clone())),
                        })
                        .eval_to_scalar()?;

                        let high = Expr::Case(Case {
                            expr: None,
                            when_then_expr: vec![(
                                Box::new(first.clone().lt_eq(second.clone())),
                                Box::new(second),
                            )],
                            else_expr: Some(Box::new(first)),
                        })
                        .eval_to_scalar()?;

                        (lit(low), lit(high))
                    }
                    v => {
                        return Err(VegaFusionError::internal(format!(
                            "values must be a two-element array. Found {v}"
                        )))
                    }
                };

                // Cast low/high scalar values to match the type of the field they will be compared to
                // Motivation: when field_dtype is Int64, and low/high are Float64, then without
                // casting, DataFusion will convert the whole field column to Float64 before running
                // the comparison.
                // We may need to revisit potential numerical precision issues at the boundaries
                let low_dtype = low.get_type(schema)?;
                let low = if is_integer_datatype(&field_dtype) && is_float_datatype(&low_dtype) {
                    cast_to(ceil(low), &field_dtype, schema)?
                } else {
                    cast_to(low, &field_dtype, schema)?
                };
                // If field is integer, and high is float, then casting will be equivalent to
                // a floor operation, so we don't handle this separately
                let high = cast_to(high, &field_dtype, schema)?;

                match self.typ {
                    SelectionType::RangeInc => Expr::Between(Between {
                        expr: Box::new(field_col),
                        negated: false,
                        low: Box::new(low),
                        high: Box::new(high),
                    }),
                    SelectionType::RangeExc => low.lt(field_col.clone()).and(field_col.lt(high)),
                    SelectionType::RangeLe => low.lt(field_col.clone()).and(field_col.lt_eq(high)),
                    SelectionType::RangeRe => low.lt_eq(field_col.clone()).and(field_col.lt(high)),
                    SelectionType::Enum => {
                        unreachable!()
                    }
                }
            }
        };

        Ok(expr)
    }

    fn cast_test_scalar(
        scalar: ScalarValue,
        field_type: &DataType,
        schema: &DFSchema,
        default_input_tz: &str,
    ) -> Result<Expr> {
        match scalar {
            ScalarValue::Utf8(Some(s))
                if parse_datetime(&s, &Some(chrono_tz::UTC)).is_some()
                    && is_numeric_datatype(field_type) =>
            {
                let timestamp_expr = Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new((*STR_TO_UTC_TIMESTAMP_UDF).clone()),
                    args: vec![lit(s), lit(default_input_tz)],
                });
                let ms_expr = Expr::ScalarUDF(expr::ScalarUDF {
                    fun: Arc::new((*UTC_TIMESTAMP_TO_EPOCH_MS).clone()),
                    args: vec![timestamp_expr],
                });
                cast_to(ms_expr, field_type, schema)
            }
            ScalarValue::Utf8(Some(s)) if field_type == &DataType::Boolean => {
                // If comparing string to boolean, treat "false" and "" as false,
                // all others as true
                Ok(match s.as_str() {
                    "false" | "" => lit(false),
                    _ => lit(true),
                })
            }
            _ => {
                if is_numeric_datatype(field_type) && !is_numeric_datatype(&scalar.data_type()) {
                    cast_to(lit(scalar), field_type, schema)
                } else {
                    Ok(lit(scalar))
                }
            }
        }
    }
}

impl TryFrom<ScalarValue> for FieldSpec {
    type Error = VegaFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Struct(Some(values), fields) => {
                let field_names: HashMap<_, _> = fields
                    .iter()
                    .enumerate()
                    .map(|(ind, f)| (f.name().clone(), ind))
                    .collect();

                // Parse field
                let field_index = field_names
                    .get("field")
                    .with_context(|| "Missing required property 'field'".to_string())?;

                let field = match values.get(*field_index) {
                    Some(ScalarValue::Utf8(Some(field))) => field.clone(),
                    _ => {
                        return Err(VegaFusionError::internal(
                            "Expected field to be a string".to_string(),
                        ))
                    }
                };

                // Parse type
                let typ_index = field_names
                    .get("type")
                    .with_context(|| "Missing required property 'type'".to_string())?;
                let typ = SelectionType::try_from(values.get(*typ_index).unwrap().clone())?;

                Ok(Self { field, typ })
            }
            _ => Err(VegaFusionError::internal(
                "Expected selection field specification to be an object".to_string(),
            )),
        }
    }
}

// Take disjunction of two expressions, potentially with optimizations
pub fn or_merge(lhs: Expr, rhs: Expr) -> Expr {
    match (lhs, rhs) {
        (
            Expr::InList(expr::InList {
                expr: lhs_expr,
                list: lhs_list,
                negated: false,
            }),
            Expr::InList(expr::InList {
                expr: rhs_expr,
                list: rhs_list,
                negated: false,
            }),
        ) if lhs_expr == rhs_expr => {
            let mut combined = lhs_list;
            combined.extend(rhs_list);
            Expr::InList(expr::InList {
                expr: lhs_expr,
                list: combined,
                negated: false,
            })
        }
        (lhs, rhs) => {
            // Use regular disjunction
            lhs.or(rhs)
        }
    }
}

/// Selection row
#[derive(Debug, Clone)]
pub struct SelectionRow {
    pub fields: Vec<FieldSpec>,
    pub values: Vec<ScalarValue>,
}

impl SelectionRow {
    pub fn to_expr(&self, schema: &DFSchema, default_input_tz: &str) -> Result<Expr> {
        let mut exprs: Vec<Expr> = Vec::new();
        for (field, value) in self.fields.iter().zip(self.values.iter()) {
            exprs.push(field.to_test_expr(value, schema, default_input_tz)?);
        }

        // Take conjunction of expressions
        Ok(exprs.into_iter().reduce(|a, b| a.and(b)).unwrap())
    }
}

impl TryFrom<ScalarValue> for SelectionRow {
    type Error = VegaFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Struct(Some(struct_values), struct_fields) => {
                let field_names: HashMap<_, _> = struct_fields
                    .iter()
                    .enumerate()
                    .map(|(ind, f)| (f.name().clone(), ind))
                    .collect();

                // Parse values
                let values_index = field_names
                    .get("values")
                    .with_context(|| "Missing required property 'values'".to_string())?;
                let values = match struct_values.get(*values_index) {
                    Some(ScalarValue::List(Some(elements), _)) => elements.clone(),
                    _ => {
                        return Err(VegaFusionError::internal(
                            "Expected 'values' to be an array".to_string(),
                        ))
                    }
                };

                // Parse fields
                let fields_index = field_names
                    .get("fields")
                    .with_context(|| "Missing required property 'fields'".to_string())?;

                let mut fields: Vec<FieldSpec> = Vec::new();
                match struct_values.get(*fields_index) {
                    Some(ScalarValue::List(Some(elements), _)) => {
                        for el in elements.iter() {
                            fields.push(FieldSpec::try_from(el.clone())?)
                        }
                    }
                    _ => {
                        return Err(VegaFusionError::internal(
                            "Expected 'values' to be an array",
                        ))
                    }
                };

                // Validate lengths
                if values.len() != fields.len() {
                    return Err(VegaFusionError::internal(format!(
                        "Length of selection fields ({}) must match that of selection values ({})\nfield: {:?}\nvalues: {:?}",
                        fields.len(),
                        values.len(),
                        fields,
                        values,
                    )));
                }

                if values.is_empty() {
                    return Err(VegaFusionError::internal("Selection fields not be empty"));
                }

                Ok(Self { values, fields })
            }
            _ => Err(VegaFusionError::internal(
                "Expected selection row specification to be an object",
            )),
        }
    }
}

fn parse_args(args: &[Expression]) -> Result<Op> {
    let n = args.len();
    if !(1..=2).contains(&n) {
        return Err(VegaFusionError::internal(format!(
            "vlSelectionTest requires 2 or 3 arguments. Received {n}"
        )));
    }

    // Validate second argument
    // ProtoExpr::Identifier(Indentifier)
    match &args[0].expr() {
        ProtoExpr::Identifier(ident) if ident.name == "datum" => {
            // All good
        }
        arg => {
            return Err(VegaFusionError::internal(format!(
                "The second argument to vlSelectionTest must be datum. Received {arg:?}"
            )))
        }
    }

    // Validate third argument and extract operation
    let op = if n < 2 {
        Op::Union
    } else {
        let arg1 = &args[1];
        match arg1.expr() {
            ProtoExpr::Literal(Literal { value: Some(Value::String(value)), .. }) => {
                // All good
                Op::from_str(value.as_str()).unwrap()
            }
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "The third argument to vlSelectionTest, if provided, must be either 'union' or 'intersect'. \
                    Received {arg1}"
                )))
            }
        }
    };
    Ok(op)
}

pub fn vl_selection_test_fn(
    table: &VegaFusionTable,
    args: &[Expression],
    schema: &DFSchema,
    tz_config: &RuntimeTzConfig,
) -> Result<Expr> {
    // Validate args and get operation
    let op = parse_args(args)?;

    // Extract vector of rows for selection dataset
    let rows = if let ScalarValue::List(Some(elements), _) = table.to_scalar_value()? {
        elements
    } else {
        unreachable!()
    };

    // Calculate selection expression for each row in selection dataset
    let mut exprs: Vec<Expr> = Vec::new();
    for row in rows {
        let row_spec = SelectionRow::try_from(row)?;
        exprs.push(row_spec.to_expr(schema, &tz_config.default_input_tz.to_string())?)
    }

    // Combine expressions according to op
    let expr = if exprs.is_empty() {
        lit(false)
    } else {
        match op {
            Op::Union => exprs.into_iter().reduce(or_merge).unwrap(),
            Op::Intersect => exprs.into_iter().reduce(|a, b| a.and(b)).unwrap(),
        }
    };

    Ok(expr)
}
