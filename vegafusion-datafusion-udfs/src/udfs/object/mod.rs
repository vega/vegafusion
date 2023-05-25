use datafusion_physical_expr::functions::make_scalar_function;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, StructArray};
use vegafusion_common::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use vegafusion_common::datafusion_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};

pub fn make_object_constructor_udf(keys: &[String], value_types: &[DataType]) -> ScalarUDF {
    // Build fields vector
    let fields: Vec<_> = keys
        .iter()
        .zip(value_types.iter())
        .map(|(k, dtype)| Field::new(k, dtype.clone(), true))
        .collect();

    let struct_dtype = DataType::Struct(Fields::from(fields.clone()));

    let object_constructor = move |args: &[ArrayRef]| {
        let pairs: Vec<_> = fields
            .iter()
            .zip(args.iter())
            .map(|(f, v)| (FieldRef::new(f.clone()), v.clone()))
            .collect();
        Ok(Arc::new(StructArray::from(pairs)) as ArrayRef)
    };

    let object_constructor = make_scalar_function(object_constructor);

    let return_type: ReturnTypeFunction = Arc::new(move |_args| Ok(Arc::new(struct_dtype.clone())));

    let name_csv: Vec<_> = keys
        .iter()
        .zip(value_types)
        .map(|(k, dtype)| format!("{k}: {dtype}"))
        .collect();

    ScalarUDF::new(
        &format!("object{{{}}}", name_csv.join(",")),
        &Signature::any(keys.len(), Volatility::Immutable),
        &return_type,
        &object_constructor,
    )
}
