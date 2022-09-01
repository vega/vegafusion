/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use std::ops::Rem;
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_plan::{DFSchema, Expr, ExprSchemable};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ColumnarValue, lit, ReturnTypeFunction, ScalarFunctionImplementation, Signature, TypeSignature, Volatility};
use std::sync::Arc;
use datafusion::common::DataFusionError;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ObjectExpression;

pub fn compile_object(
    node: &ObjectExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let mut args: Vec<Expr> = Vec::new();

    for prop in &node.properties {
        let expr = compile(prop.value(), config, Some(schema))?;
        let name = prop.key().to_object_key_string();
        args.push(lit(name));
        args.push(expr);
    }

    let udf = make_object_constructor_udf();

    Ok(Expr::ScalarUDF {
        fun: Arc::new(udf),
        args,
    })
}


pub fn make_object_constructor_udf() -> ScalarUDF {
    let object_constructor: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        if args.len().rem(2) != 0 {
            return Err(DataFusionError::Internal("make_struct expects an even number of arguments".to_string()))
        }

        // Collect field names
        let mut field_names: Vec<String> = Vec::new();
        for field_index in 0..(args.len() / 2) {
            let field_name_index = field_index * 2;
            if let ColumnarValue::Scalar(field_name) = &args[field_name_index] {
                field_names.push(field_name.to_string());
            } else {
                return Err(DataFusionError::Internal(
                    "make_struct expects an even numbered arguments to be field name strings".to_string())
                )
            }
        }

        // Compute broadcast length
        let mut len = None;
        for field_index in 0..(args.len() / 2) {
            let field_val_index = field_index * 2 + 1;
            if let ColumnarValue::Array(array) = &args[field_val_index] {
                len = Some(array.len());
            }
        }
        let len = len.unwrap_or(1);

        // Collect field values
        let mut field_values: Vec<(Field, ArrayRef)> = Vec::new();
        for field_index in 0..(args.len() / 2) {
            // Extract field name as string
            let field_name_index = field_index * 2;
            let field_name = if let ColumnarValue::Scalar(field_name) = &args[field_name_index] {
                field_name.to_string()
            } else {
                return Err(DataFusionError::Internal(
                    "make_struct expects an even numbered arguments to be field name strings".to_string())
                )
            };

            // Extract data type to build filed
            let field_val_index = field_index * 2 + 1;
            let field_val = &args[field_val_index];
            let dtype = field_val.data_type();
            let field = Field::new(&field_name, dtype, true);

            let field_val_array = match field_val {
                ColumnarValue::Array(array) => {
                    field_values.push((field, array.clone()))
                }
                ColumnarValue::Scalar(scalar) => {
                    field_values.push((field, scalar.to_array_of_size(len)))
                }
            };
        }

        let struct_array = Arc::new(StructArray::from(field_values)) as ArrayRef;

        Ok(ColumnarValue::Array(struct_array))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |dtypes| {
        // Note: we don't have access to the field names here, only the argument types.
        // So we use dummy c{i} field names
        let mut fields: Vec<Field> = Vec::new();
        for field_index in 0..(dtypes.len() / 2) {
            let field_val_index = field_index * 2 + 1;
            let field_val_dtype = &dtypes[field_val_index];
            let field_name = format!("c{}", field_index);
            let field = Field::new(&field_name, field_val_dtype.clone(), true);
            fields.push(field);
        }
        Ok(Arc::new(DataType::Struct(fields)))
    });

    // Accept up to 10 fields
    let type_sigs: Vec<_> = (0..=10).map(|v| TypeSignature::Any(v*2)).collect();

    let signature = Signature::one_of(type_sigs, Volatility::Immutable);

    ScalarUDF::new(
        "make_struct",
        &signature,
        &return_type,
        &object_constructor,
    )
}
