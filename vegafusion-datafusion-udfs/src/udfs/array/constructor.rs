use datafusion_physical_expr::functions::make_scalar_function;
use std::sync::Arc;
use std::vec;
use vegafusion_common::arrow::array::{
    Array, ArrayDataBuilder, ArrayRef, BooleanBufferBuilder, Int32Array, ListArray,
};
use vegafusion_common::arrow::datatypes::{DataType, Field, Float64Type};
use vegafusion_common::datafusion_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};

pub fn make_array_constructor_udf() -> ScalarUDF {
    let array_constructor = |args: &[ArrayRef]| {
        // Signature ensures arguments have the same type and length
        let num_args = args.len();

        // Return empty array as single row, empty array with Float64 data type
        if num_args == 0 || num_args == 1 && args[0].data_type() == &DataType::Null {
            let empty_row = vec![Some(Vec::<Option<f64>>::new())];
            let empty = ListArray::from_iter_primitive::<Float64Type, _, _>(empty_row);
            return Ok(Arc::new(empty) as ArrayRef);
        }

        let num_rows = args[0].len();
        let element_dtype = args[0].data_type();
        let array_dtype = DataType::List(Arc::new(Field::new("item", element_dtype.clone(), true)));

        // Concatenate arrays into single flat array (in column-major ordering)
        let arrays: Vec<&dyn Array> = args.iter().map(|a| a.as_ref()).collect();
        let concatted = vegafusion_common::arrow::compute::concat(&arrays).unwrap();

        // Transpose array into row-major ordering and compute offset indices
        let mut indices_builder = Int32Array::builder(num_rows * num_args);
        let mut flat_valid_builder = BooleanBufferBuilder::new(num_rows * num_args);
        let mut offsets_builder = Int32Array::builder(num_rows + 1);

        offsets_builder.append_value(0);

        for r in 0..num_rows {
            for (a, arg) in args.iter().enumerate() {
                let col_major_idx = (a * num_rows + r) as i32;
                indices_builder.append_value(col_major_idx);
                flat_valid_builder.append(arg.is_valid(r));
            }
            offsets_builder.append_value((num_args * (r + 1)) as i32);
        }

        let flat_values = vegafusion_common::arrow::compute::take(
            concatted.as_ref(),
            &indices_builder.finish(),
            Default::default(),
        )
        .unwrap();
        let offsets = offsets_builder.finish();

        // Build ListArray data
        let list_array_data = ArrayDataBuilder::new(array_dtype)
            .len(num_rows)
            .null_bit_buffer(Some(flat_valid_builder.finish().into_inner()))
            .add_buffer(offsets.to_data().buffers()[0].clone())
            .add_child_data(flat_values.to_data())
            .build()?;

        Ok(Arc::new(ListArray::from(list_array_data)) as ArrayRef)
    };
    let array_constructor = make_scalar_function(array_constructor);

    let return_type: ReturnTypeFunction = Arc::new(move |args| {
        Ok(Arc::new(DataType::List(Arc::new(Field::new(
            "item",
            if args.is_empty() {
                DataType::Float64
            } else {
                args[0].clone()
            },
            true,
        )))))
    });
    ScalarUDF::new(
        "make_list",
        &Signature::variadic_equal(Volatility::Immutable),
        &return_type,
        &array_constructor,
    )
}

lazy_static! {
    pub static ref ARRAY_CONSTRUCTOR_UDF: ScalarUDF = make_array_constructor_udf();
}
