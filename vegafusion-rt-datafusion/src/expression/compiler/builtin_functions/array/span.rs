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
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::physical_plan::functions::{
    ReturnTypeFunction, ScalarFunctionImplementation, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValueHelpers;

/// `span(array)`
///
/// Returns the span of array: the difference between the last and first elements,
/// or array[array.length-1] - array[0].
///
/// See https://vega.github.io/vega/docs/expressions/#span
pub fn make_span_udf() -> ScalarUDF {
    let span_fn: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                match value {
                    ScalarValue::Float64(_) => {
                        ColumnarValue::Scalar(ScalarValue::try_from(&DataType::Float64).unwrap())
                    }
                    ScalarValue::List(Some(arr), element_type) => {
                        match element_type.as_ref() {
                            DataType::Float64 => {
                                if arr.is_empty() {
                                    // Span of empty array is null
                                    ColumnarValue::Scalar(
                                        ScalarValue::try_from(&DataType::Float64).unwrap(),
                                    )
                                } else {
                                    let first = arr.first().unwrap().to_f64().unwrap();
                                    let last = arr.last().unwrap().to_f64().unwrap();
                                    ColumnarValue::Scalar(ScalarValue::from(last - first))
                                }
                            }
                            _ => {
                                panic!(
                                    "Unexpected element type for span function: {}",
                                    element_type
                                )
                            }
                        }
                    }
                    _ => {
                        panic!("Unexpected type passed to span: {}", value)
                    }
                }
            }
            ColumnarValue::Array(_array) => {
                todo!("Span on column not yet implemented")
            }
        })
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
    ScalarUDF::new(
        "span",
        &Signature::uniform(
            1,
            vec![
                DataType::Float64, // For null
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &span_fn,
    )
}
