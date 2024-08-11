use datafusion_common::utils::array_into_list_array;
use datafusion_expr::{lit, Expr};
use itertools::Itertools;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::data::scalar::{ArrayRefHelpers, ScalarValueHelpers};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_common::error::{Result, VegaFusionError};

use vegafusion_core::proto::gen::expression::literal::Value;

use crate::expression::compiler::builtin_functions::data::vl_selection_test::{
    SelectionRow, SelectionType,
};

use crate::task_graph::timezone::RuntimeTzConfig;
use vegafusion_core::proto::gen::{
    expression::expression::Expr as ProtoExpr, expression::Expression, expression::Literal,
};

use super::vl_selection_test::Op;

pub fn parse_args(args: &[Expression]) -> Result<Op> {
    let n = args.len();
    if !(0..=1).contains(&n) {
        return Err(VegaFusionError::internal(format!(
            "vlSelectionResolve requires 1 or 2 arguments. Received {n}"
        )));
    }

    // Validate second argument and extract operation
    let op = if n < 1 {
        Op::Union
    } else {
        let arg1 = &args[0];
        match arg1.expr() {
            ProtoExpr::Literal(Literal { value: Some(Value::String(value)), .. }) => {
                // All good
                Op::from_str(value.as_str()).unwrap()
            }
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "The second argument to vlSelectionResolve, if provided, must be either 'union' or 'intersect'. \
                    Received {arg1}"
                )))
            }
        }
    };
    Ok(op)
}

pub fn vl_selection_resolve_fn(
    table: &VegaFusionTable,
    args: &[Expression],
    _schema: &DFSchema,
    _tz_config: &RuntimeTzConfig,
) -> Result<Expr> {
    // Validate args and get operation
    let _op = parse_args(args)?;

    // Extract vector of rows for selection dataset
    let rows = if let ScalarValue::List(array) = table.to_scalar_value()? {
        array.value(0).to_scalar_vec()?
    } else {
        unreachable!()
    };

    // let mut prop_names: Vec<String> = Vec::new();
    // let mut prop_values: Vec<ScalarValue> = Vec::new();

    let mut props: HashMap<String, Vec<ScalarValue>> = HashMap::new();

    for row in rows {
        let row_spec = SelectionRow::try_from(row)?;
        for (field, value) in row_spec.fields.iter().zip(&row_spec.values) {
            let value = match field.typ {
                SelectionType::Enum => {
                    if let ScalarValue::List(array) = value {
                        array.value(0).to_scalar_vec()?
                    } else {
                        vec![value.clone()]
                    }
                }
                _ => {
                    match &value {
                        ScalarValue::List(array) if array.value(0).len() == 2 => {
                            // Don't assume elements are in ascending order
                            let elements = array.value(0).to_scalar_vec()?;
                            let first = elements[0].to_f64()?;
                            let second = elements[1].to_f64()?;

                            let (low, high) = if first <= second {
                                (first, second)
                            } else {
                                (second, first)
                            };
                            vec![ScalarValue::from(low), ScalarValue::from(high)]
                        }
                        v => {
                            return Err(VegaFusionError::internal(format!(
                                "values must be a two-element array. Found {v}"
                            )))
                        }
                    }
                }
            };

            let values = props.entry(field.field.clone()).or_default();
            values.extend(value.clone());
        }
    }

    let props = props
        .into_iter()
        .map(|(name, values)| {
            // Turn values into a scalar list
            let values = ScalarValue::List(Arc::new(array_into_list_array(
                ScalarValue::iter_to_array(values)?,
                true,
            )));
            Ok((name, values))
        })
        .collect::<Result<Vec<_>>>()?;
    let props: Vec<_> = props
        .iter()
        .sorted_by_key(|(n, _)| n.clone())
        .collect::<Vec<_>>();
    let props: Vec<_> = props
        .iter()
        .map(|(name, value)| (name.as_str(), value.clone()))
        .collect();

    let object_result = ScalarValue::from(props);
    Ok(lit(object_result))
}
