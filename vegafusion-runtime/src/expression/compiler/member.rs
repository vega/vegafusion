use crate::expression::compiler::builtin_functions::array::length::length_transform;
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::ExprHelpers;
use datafusion_expr::{lit, Expr};
use datafusion_functions::expr_fn::{get_field, substring};
use datafusion_functions_nested::expr_fn::array_element;
use std::convert::TryFrom;
use vegafusion_common::arrow::array::Int64Array;
use vegafusion_common::arrow::compute::cast;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::column::flat_col;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_common::datatypes::{data_type, is_numeric_datatype};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::expression::{Identifier, MemberExpression};

pub fn compile_member(
    node: &MemberExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // Maybe an numeric array index
    let mut index: Option<usize> = None;

    // Get string-form of index
    let property_string = if node.computed {
        // e.g. foo[val]
        let compiled_property = compile(node.property(), config, Some(schema))?;
        let evaluated_property = compiled_property.eval_to_scalar().with_context(
            || format!("VegaFusion does not support the use of datum expressions in object member access: {node}")
        )?;
        let prop_str = evaluated_property.to_string();
        if is_numeric_datatype(&evaluated_property.data_type()) {
            let int_array = cast(&evaluated_property.to_array()?, &DataType::Int64).unwrap();
            let int_array = int_array.as_any().downcast_ref::<Int64Array>().unwrap();
            index = Some(int_array.value(0) as usize);
        } else {
            // Try to convert string to number
            if let Ok(v) = prop_str.parse::<f64>() {
                // Then case to usize
                index = Some(v as usize);
            }
        }
        prop_str
    } else if let Ok(property) = node.property().as_identifier() {
        property.name.clone()
    } else {
        return Err(VegaFusionError::compilation(format!(
            "Invalid membership property: {}",
            node.property()
        )));
    };

    // Handle datum property access. These represent DataFusion column expressions
    match node.object().as_identifier() {
        Ok(Identifier { name, .. }) if name == "datum" => {
            return if schema.field_with_unqualified_name(&property_string).is_ok() {
                let col_expr = flat_col(&property_string);
                Ok(col_expr)
            } else {
                // Column not in schema, evaluate to scalar null
                Ok(lit(ScalarValue::Boolean(None)))
            };
        }
        _ => {}
    }

    let compiled_object = compile(node.object(), config, Some(schema))?;
    let dtype = data_type(&compiled_object, schema)?;

    let expr = match dtype {
        DataType::Struct(ref fields) => {
            if fields.iter().any(|f| f.name() == &property_string) {
                get_field(compiled_object, property_string)
            } else {
                // Property does not exist, return null
                return Ok(lit(ScalarValue::try_from(&DataType::Float64).unwrap()));
            }
        }
        _ => {
            if property_string == "length" {
                length_transform(&[compiled_object], schema)?
            } else if matches!(dtype, DataType::Utf8 | DataType::LargeUtf8) {
                if let Some(index) = index {
                    // SQL substr function is 1-indexed so add one
                    substring(compiled_object, lit((index + 1) as i32), lit(1i64))
                } else {
                    return Err(VegaFusionError::compilation(format!(
                        "Non-numeric element index: {property_string}"
                    )));
                }
            } else if matches!(dtype, DataType::List(_) | DataType::FixedSizeList(_, _)) {
                if let Some(index) = index {
                    array_element(compiled_object, lit((index + 1) as i32))
                } else {
                    return Err(VegaFusionError::compilation(format!(
                        "Non-numeric element index: {property_string}"
                    )));
                }
            } else {
                // Invalid target of member access (e.g. null float64). Return NULL
                return Ok(lit(ScalarValue::Float64(None)));
            }
        }
    };

    Ok(expr)
}
