/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_plan::{DFSchema, Expr, ExprSchemable};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion_expr::{ReturnTypeFunction, Signature, Volatility};
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::ObjectExpression;

pub fn compile_object(
    node: &ObjectExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<Expr> = Vec::new();
    let mut value_types: Vec<DataType> = Vec::new();
    for prop in &node.properties {
        let expr = compile(prop.value(), config, Some(schema))?;
        let name = prop.key().to_object_key_string();
        keys.push(name);
        value_types.push(expr.get_type(schema)?);
        values.push(expr)
    }

    let udf = make_object_constructor_udf(keys.as_slice(), value_types.as_slice());

    Ok(Expr::ScalarUDF {
        fun: Arc::new(udf),
        args: values,
    })
}

pub fn make_object_constructor_udf(keys: &[String], value_types: &[DataType]) -> ScalarUDF {
    // Build fields vector
    let fields: Vec<_> = keys
        .iter()
        .zip(value_types.iter())
        .map(|(k, dtype)| Field::new(k, dtype.clone(), false))
        .collect();

    let struct_dtype = DataType::Struct(fields.clone());

    let object_constructor = move |args: &[ArrayRef]| {
        let pairs: Vec<_> = fields
            .iter()
            .zip(args.iter())
            .map(|(f, v)| (f.clone(), v.clone()))
            .collect();
        Ok(Arc::new(StructArray::from(pairs)) as ArrayRef)
    };

    let object_constructor = make_scalar_function(object_constructor);

    let return_type: ReturnTypeFunction = Arc::new(move |_args| Ok(Arc::new(struct_dtype.clone())));

    let name_csv: Vec<_> = keys
        .iter()
        .zip(value_types)
        .map(|(k, dtype)| format!("{}: {}", k, dtype))
        .collect();

    ScalarUDF::new(
        &format!("object{{{}}}", name_csv.join(",")),
        &Signature::any(keys.len(), Volatility::Immutable),
        &return_type,
        &object_constructor,
    )
}
