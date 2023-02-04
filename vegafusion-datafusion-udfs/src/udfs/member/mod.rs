use datafusion_physical_expr::functions::make_scalar_function;
use std::sync::Arc;
use vegafusion_common::arrow::array::{
    new_null_array, Array, ArrayRef, Int32Array, ListArray, StructArray,
};
use vegafusion_common::arrow::compute::kernels;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    Volatility,
};
use vegafusion_common::error::{Result, VegaFusionError};

pub fn make_get_object_member_udf(
    object_type: &DataType,
    property_name: &str,
) -> Result<ScalarUDF> {
    let (field_index, field_type) = if let DataType::Struct(fields) = object_type {
        match fields
            .iter()
            .enumerate()
            .find(|(_i, f)| f.name() == property_name)
        {
            Some((field_index, field)) => (field_index, field.data_type().clone()),
            None => {
                return Err(VegaFusionError::compilation(format!(
                    "No object property named {property_name}"
                )))
            }
        }
    } else {
        return Err(VegaFusionError::compilation(
            "Target of object property access is not a Struct type",
        ));
    };

    let get = move |args: &[ArrayRef]| {
        // Signature ensures there is a single argument
        let object = &args[0];

        let struct_array = object.as_any().downcast_ref::<StructArray>().unwrap();
        Ok(struct_array.column(field_index).clone())
    };
    let get = make_scalar_function(get);

    let return_type: ReturnTypeFunction =
        Arc::new(move |_dtype: &[DataType]| Ok(Arc::new(field_type.clone())));

    Ok(ScalarUDF::new(
        &format!("get[{property_name}]"),
        &Signature::exact(vec![object_type.clone()], Volatility::Immutable),
        &return_type,
        &get,
    ))
}

pub fn make_get_element_udf(index: i32) -> ScalarUDF {
    let get_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                match value {
                    ScalarValue::List(Some(arr), element_dtype) => {
                        match arr.get(index as usize) {
                            Some(element) => {
                                // Scalar element of list
                                ColumnarValue::Scalar(element.clone())
                            }
                            None => {
                                // out of bounds, null
                                ColumnarValue::Scalar(
                                    ScalarValue::try_from(element_dtype.data_type()).unwrap(),
                                )
                            }
                        }
                    }
                    _ => {
                        // null
                        ColumnarValue::Scalar(ScalarValue::try_from(&DataType::Float64).unwrap())
                    }
                }
            }
            ColumnarValue::Array(array) => {
                match array.data_type() {
                    DataType::List(_) => {
                        // There is not substring-like kernel for general list arrays.
                        // So instead, build indices into the flat buffer and use take
                        let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                        let mut take_index_builder = Int32Array::builder(array.len());
                        let offsets = array.value_offsets();
                        let _flat_values = array.values();

                        for i in 0..array.len() {
                            let el_start = offsets[i];
                            let el_stop = offsets[i + 1];
                            if el_start + index < el_stop {
                                take_index_builder.append_value(el_start + index);
                            } else {
                                take_index_builder.append_null();
                            }
                        }

                        let result = kernels::take::take(
                            array.values().as_ref(),
                            &take_index_builder.finish(),
                            Default::default(),
                        )
                        .unwrap();

                        ColumnarValue::Array(result)
                    }
                    _ => ColumnarValue::Array(new_null_array(&DataType::Float64, array.len())),
                }
            }
        })
    });

    let return_type: ReturnTypeFunction = Arc::new(move |dtype| {
        let new_dtype = match &dtype[0] {
            DataType::List(field) => field.data_type().clone(),
            dtype => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported datatype for get index {dtype:?}"
                )))
            }
        };
        Ok(Arc::new(new_dtype))
    });
    ScalarUDF::new(
        &format!("get[{index}]"),
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &get_fn,
    )
}
