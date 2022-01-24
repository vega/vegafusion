// VegaFusion
// Copyright (C) 2022, Jon Mease
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use chrono::{Local, NaiveDateTime, TimeZone};
use datafusion::arrow::array::{Int64Array, TimestampMillisecondArray};
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use std::sync::Arc;
use vegafusion_core::arrow::array::ArrayRef;
use vegafusion_core::arrow::compute::unary;
use vegafusion_core::arrow::datatypes::{DataType, TimeUnit};

lazy_static! {
    pub static ref LOCAL_TO_UTC_MILLIS: ScalarUDF = make_to_utc_millis_fn();
}

pub fn make_to_utc_millis_fn() -> ScalarUDF {
    let to_utc_millis_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single string argument
        let arg = &args[0];
        let date_strs = arg
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        let array: Int64Array = unary(date_strs, |v| {
            // Build naive datetime for time
            let seconds = v / 1000;
            let milliseconds = v % 1000;
            let nanoseconds = (milliseconds * 1_000_000) as u32;
            let naive_datetime = NaiveDateTime::from_timestamp(seconds, nanoseconds);

            // Get UTC offset when the naive datetime is considered to be in local time
            let local = Local {};
            let local_datetime = local
                .from_local_datetime(&naive_datetime)
                .earliest()
                .unwrap();
            local_datetime.timestamp_millis()
        });
        Ok(Arc::new(array) as ArrayRef)
    };

    let to_utc_millis_fn = make_scalar_function(to_utc_millis_fn);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "to_utc_millis_fn",
        &Signature::uniform(
            1,
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &to_utc_millis_fn,
    )
}
