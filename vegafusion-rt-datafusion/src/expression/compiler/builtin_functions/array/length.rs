/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use datafusion::arrow::array::{new_null_array, Array, Int32Array, ListArray};
use datafusion::arrow::compute::kernels;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::{
    ReturnTypeFunction, ScalarFunctionImplementation, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::convert::TryFrom;
use std::sync::Arc;

/// `length(array)`
///
/// Returns the length of the input array or string
///
/// See https://vega.github.io/vega/docs/expressions/#length
pub fn make_length_udf() -> ScalarUDF {
    let length_fn: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                match value {
                    ScalarValue::List(Some(arr), _) => {
                        ColumnarValue::Scalar(ScalarValue::from(arr.len() as i32))
                    }
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        ColumnarValue::Scalar(ScalarValue::from(s.len() as i32))
                    }
                    _ => {
                        // Scalar i32 null
                        ColumnarValue::Scalar(ScalarValue::try_from(&DataType::Int32).unwrap())
                    }
                }
            }
            ColumnarValue::Array(array) => {
                match array.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 => {
                        // String length
                        ColumnarValue::Array(kernels::length::length(array.as_ref()).unwrap())
                    }
                    DataType::FixedSizeList(_, n) => {
                        // Use scalar length
                        ColumnarValue::Scalar(ScalarValue::from(*n))
                    }
                    DataType::List(_) => {
                        let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                        let offsets = array.value_offsets();
                        let mut length_builder = Int32Array::builder(array.len());

                        for i in 0..array.len() {
                            length_builder
                                .append_value((offsets[i + 1] - offsets[i]) as i32)
                                .unwrap();
                        }

                        ColumnarValue::Array(Arc::new(length_builder.finish()))
                    }
                    _ => {
                        // Array of i32 nulls
                        ColumnarValue::Array(new_null_array(&DataType::Int32, array.len()))
                    }
                }
            }
        })
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));
    ScalarUDF::new(
        "length",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &length_fn,
    )
}
