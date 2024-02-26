use std::any::Any;
use vegafusion_common::arrow::array::{new_null_array, Array, Int32Array, ListArray, StructArray};
use vegafusion_common::arrow::compute::kernels;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use vegafusion_common::error::{Result, VegaFusionError};

#[derive(Debug, Clone)]
pub struct GetObjectMemberUDF {
    field_type: DataType,
    field_index: usize,
    signature: Signature,
    name: String,
}

impl GetObjectMemberUDF {
    pub fn new(property_name: String, object_type: DataType) -> Result<Self> {
        let signature = Signature::exact(vec![object_type.clone()], Volatility::Immutable);
        let name = format!("get[{property_name}]");

        let (field_index, field_type) = if let DataType::Struct(fields) = &object_type {
            match fields
                .iter()
                .enumerate()
                .find(|(_i, f)| f.name() == &property_name)
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

        Ok(Self {
            field_type,
            field_index,
            signature,
            name,
        })
    }
}

impl ScalarUDFImpl for GetObjectMemberUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> std::result::Result<DataType, DataFusionError> {
        Ok(self.field_type.clone())
    }

    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        // Signature ensures there is a single argument
        let arg = &args[0];

        match arg {
            ColumnarValue::Scalar(ScalarValue::Struct(arg)) => {
                let c = arg.column(self.field_index);
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(c, 0)?))
            }
            ColumnarValue::Array(object) => {
                let struct_array = object.as_any().downcast_ref::<StructArray>().unwrap();
                Ok(ColumnarValue::Array(
                    struct_array.column(self.field_index).clone(),
                ))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unexpected object type for member access: {:?}",
                arg.data_type()
            ))),
        }
    }
}

pub fn make_get_object_member_udf(
    object_type: &DataType,
    property_name: &str,
) -> Result<ScalarUDF> {
    Ok(ScalarUDF::from(GetObjectMemberUDF::new(
        property_name.to_string(),
        object_type.clone(),
    )?))
}

#[derive(Debug, Clone)]
pub struct ArrayElementUDF {
    name: String,
    signature: Signature,
    index: i32,
}

impl ArrayElementUDF {
    pub fn new(index: i32) -> Self {
        let signature = Signature::any(1, Volatility::Immutable);
        Self {
            name: format!("get[{index}]"),
            signature,
            index,
        }
    }
}

impl ScalarUDFImpl for ArrayElementUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        arg_types: &[DataType],
    ) -> std::result::Result<DataType, DataFusionError> {
        let new_dtype = match &arg_types[0] {
            DataType::List(field) => field.data_type().clone(),
            dtype => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported datatype for get index {dtype:?}"
                )))
            }
        };
        Ok(new_dtype)
    }

    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> std::result::Result<ColumnarValue, DataFusionError> {
        // Signature ensures there is a single argument
        let arg = &args[0];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                match value {
                    ScalarValue::List(arr) => {
                        let arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                        match ScalarValue::try_from_array(&arr.value(0), self.index as usize) {
                            Ok(element) => {
                                // Scalar element of list
                                ColumnarValue::Scalar(element.clone())
                            }
                            _ => {
                                // out of bounds, null
                                ColumnarValue::Scalar(ScalarValue::try_from(arr.data_type())?)
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
                            if el_start + self.index < el_stop {
                                take_index_builder.append_value(el_start + self.index);
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
    }
}

pub fn make_get_element_udf(index: i32) -> ScalarUDF {
    ScalarUDF::from(ArrayElementUDF::new(index))
}
