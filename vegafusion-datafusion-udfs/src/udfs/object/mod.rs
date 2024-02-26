use crate::udfs::util::make_scalar_function;
use std::any::Any;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, StructArray};
use vegafusion_common::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use vegafusion_common::datafusion_common::DataFusionError;
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone)]
struct ObjectConstructorUDF {
    signature: Signature,
    fields: Vec<Field>,
    struct_dtype: DataType,
    name: String,
}

impl ObjectConstructorUDF {
    pub fn new(keys: &[String], value_types: &[DataType]) -> Self {
        // Build fields vector
        let fields: Vec<_> = keys
            .iter()
            .zip(value_types.iter())
            .map(|(k, dtype)| Field::new(k, dtype.clone(), true))
            .collect();
        let struct_dtype = DataType::Struct(Fields::from(fields.clone()));

        // Build name
        let name_csv: Vec<_> = keys
            .iter()
            .zip(value_types)
            .map(|(k, dtype)| format!("{k}: {dtype}"))
            .collect();
        let name = format!("object{{{}}}", name_csv.join(","));
        Self {
            signature: Signature::any(keys.len(), Volatility::Immutable),
            fields,
            struct_dtype,
            name,
        }
    }
}

impl ScalarUDFImpl for ObjectConstructorUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.struct_dtype.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        let fields = self.fields.clone();
        let object_constructor = move |args: &[ArrayRef]| {
            let pairs: Vec<_> = fields
                .iter()
                .zip(args.iter())
                .map(|(f, v)| (FieldRef::new(f.clone()), v.clone()))
                .collect();
            Ok(Arc::new(StructArray::from(pairs)) as ArrayRef)
        };
        let object_constructor = make_scalar_function(object_constructor);
        object_constructor(args)
    }
}

pub fn make_object_constructor_udf(keys: &[String], value_types: &[DataType]) -> ScalarUDF {
    ScalarUDF::from(ObjectConstructorUDF::new(keys, value_types))
}
