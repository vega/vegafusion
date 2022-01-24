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
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use regex::Regex;
use std::sync::Arc;

lazy_static! {
    pub static ref DATETIME_TO_MILLIS_LOCAL: ScalarUDF =
        make_date_str_to_millis_udf(DateParseMode::Local);
    pub static ref DATETIME_TO_MILLIS_UTC: ScalarUDF =
        make_date_str_to_millis_udf(DateParseMode::Utc);
    pub static ref DATETIME_TO_MILLIS_JAVASCRIPT: ScalarUDF =
        make_date_str_to_millis_udf(DateParseMode::JavaScript);
}

#[derive(Debug, Copy, Clone)]
pub enum DateParseMode {
    Local,
    Utc,
    JavaScript,
}

pub fn get_datetime_udf(mode: DateParseMode) -> ScalarUDF {
    match mode {
        DateParseMode::Local => DATETIME_TO_MILLIS_LOCAL.clone(),
        DateParseMode::Utc => DATETIME_TO_MILLIS_UTC.clone(),
        DateParseMode::JavaScript => DATETIME_TO_MILLIS_JAVASCRIPT.clone(),
    }
}

/// Parse a more generous specification of the iso 8601 date standard
/// Allow omission of time components
pub fn parse_datetime(date_str: &str, mode: DateParseMode) -> Option<DateTime<FixedOffset>> {
    let mut date_tokens = vec![String::from(""), String::from(""), String::from("")];
    let mut time_tokens = vec![
        String::from(""),
        String::from(""),
        String::from(""),
        String::from(""),
    ];
    let mut timezone_tokens = vec![String::from(""), String::from("")];
    let mut timezone_sign = ' ';
    let mut date_ind = 0;
    let mut time_ind = 0;
    let mut timezone_ind = 0;
    let mut stage = 0;
    let mut has_time = false;
    let mut date_split = '-';

    // tokenize date string
    for c in date_str.trim().chars() {
        match stage {
            0 => {
                // Parsing date
                if date_ind < 2 && (c == '-' || c == '/' || c == ' ') {
                    date_split = c;
                    date_ind += 1;
                } else if date_ind == 2 && (c == 'T' || c == ' ') {
                    // Move on to time portion
                    stage += 1;
                } else if c.is_ascii_alphanumeric() {
                    date_tokens[date_ind].push(c)
                } else {
                    return None;
                }
            }
            1 => {
                // Parsing time
                if c.is_whitespace() {
                    continue;
                } else if c.is_digit(10) {
                    has_time = true;
                    time_tokens[time_ind].push(c)
                } else if (time_ind < 2 && c == ':') || (time_ind == 2 && c == '.') {
                    // Move on to next portion
                    time_ind += 1;
                } else if c == '+' || c == '-' {
                    // Move on to time zone
                    stage += 1;

                    // Save sign of offset hour
                    timezone_sign = c;
                } else if c == 'Z' {
                    // Done, UTC 0
                    timezone_tokens[0].push('0');
                    timezone_tokens[1].push('0');
                    break;
                } else {
                    return None;
                }
            }
            2 => {
                // Parsing timezone
                if c.is_digit(10) {
                    timezone_tokens[timezone_ind].push(c)
                } else if timezone_ind == 0 && c == ':' {
                    timezone_ind += 1;
                } else {
                    // String should have ended
                    return None;
                }
            }
            _ => return None,
        }
    }

    // determine which date token holds year, month, and date
    let year_re = Regex::new(r"\d{4}").unwrap();

    let (year, month, day, iso8601_date) = if year_re.is_match(&date_tokens[0]) {
        // Assume YYYY-MM-DD (where '-' can also be '/' or ' ')
        // Year parsing needs to succeed, or we fail. All other components are optional
        let year: i32 = date_tokens[0].parse().ok()?;
        let month: u32 = parse_month_str(&date_tokens[1]).unwrap_or(1);
        let day: u32 = date_tokens[2].parse().unwrap_or(1);
        (year, month, day, date_split == '-')
    } else if year_re.is_match(&date_tokens[2]) {
        // Assume MM/DD/YYYY (where '/' can also be '-' or ' ')
        let year: i32 = date_tokens[2].parse().ok()?;
        let month: u32 = parse_month_str(&date_tokens[0]).unwrap_or(1);
        let day: u32 = date_tokens[1].parse().ok()?;
        (year, month, day, false)
    } else {
        // 4-digit year may be the first of third date component
        return None;
    };

    let hour: u32 = time_tokens[0].parse().unwrap_or(0);
    let minute: u32 = time_tokens[1].parse().unwrap_or(0);
    let second: u32 = time_tokens[2].parse().unwrap_or(0);
    let milliseconds: u32 = if time_tokens[3].is_empty() {
        0
    } else if time_tokens[3].len() == 3 {
        time_tokens[3].parse().ok()?
    } else {
        return None;
    };

    let offset = if timezone_tokens[0].is_empty() {
        match mode {
            DateParseMode::Utc => FixedOffset::east(0),
            DateParseMode::JavaScript if iso8601_date && !has_time => FixedOffset::east(0),
            _ => {
                // Treat date as in local timezone
                let local = Local {};
                // No timezone specified, build NaiveDateTime
                let naive_date = NaiveDate::from_ymd(year, month, day);
                let naive_time = NaiveTime::from_hms_milli(hour, minute, second, milliseconds);
                let naive_date = NaiveDateTime::new(naive_date, naive_time);

                local.offset_from_local_datetime(&naive_date).single()?
            }
        }
    } else {
        let timezone_hours: i32 = timezone_tokens[0].parse().unwrap_or(0);
        let timezone_minutes: i32 = timezone_tokens[1].parse().unwrap_or(0);
        let time_offset_seconds = timezone_hours * 3600 + timezone_minutes * 60;

        if timezone_sign == '-' {
            FixedOffset::west(time_offset_seconds)
        } else {
            FixedOffset::east(time_offset_seconds)
        }
    };

    let parsed = offset
        .ymd(year, month, day)
        .and_hms_milli(hour, minute, second, milliseconds);
    Some(parsed)
}

fn parse_month_str(month_str: &str) -> Option<u32> {
    // try parsing as integer
    let month_str = month_str.to_lowercase();
    if let Ok(month) = month_str.parse::<u32>() {
        Some(month)
    } else if month_str.len() > 2 {
        // Try parsing as month name
        if "january"[..month_str.len()] == month_str {
            Some(1)
        } else if "february"[..month_str.len()] == month_str {
            Some(2)
        } else if "march"[..month_str.len()] == month_str {
            Some(3)
        } else if "april"[..month_str.len()] == month_str {
            Some(4)
        } else if "may"[..month_str.len()] == month_str {
            Some(5)
        } else if "june"[..month_str.len()] == month_str {
            Some(6)
        } else if "july"[..month_str.len()] == month_str {
            Some(7)
        } else if "august"[..month_str.len()] == month_str {
            Some(8)
        } else if "september"[..month_str.len()] == month_str {
            Some(9)
        } else if "october"[..month_str.len()] == month_str {
            Some(10)
        } else if "november"[..month_str.len()] == month_str {
            Some(11)
        } else if "december"[..month_str.len()] == month_str {
            Some(12)
        } else {
            None
        }
    } else {
        None
    }
}

pub fn parse_datetime_to_utc_millis(date_str: &str, mode: DateParseMode) -> Option<i64> {
    // Parse to datetime
    let parsed = parse_datetime(date_str, mode)?;

    // Convert to UTC
    let parsed_utc = parsed.with_timezone(&Utc);

    // Extract milliseconds
    Some(parsed_utc.timestamp_millis())
}

pub fn make_date_str_to_millis_udf(mode: DateParseMode) -> ScalarUDF {
    let to_millis_fn = move |args: &[ArrayRef]| {
        // Signature ensures there is a single string argument
        let arg = &args[0];
        let date_strs = arg.as_any().downcast_ref::<StringArray>().unwrap();
        Ok(datetime_strs_to_millis(date_strs, mode))
    };

    let to_millis_fn = make_scalar_function(to_millis_fn);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "vf_datetime_to_millis",
        &Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        &return_type,
        &to_millis_fn,
    )
}

pub fn datetime_strs_to_millis(date_strs: &StringArray, mode: DateParseMode) -> ArrayRef {
    let millis_array = Int64Array::from(
        date_strs
            .iter()
            .map(|date_str| -> Option<i64> {
                date_str.and_then(|date_str| parse_datetime_to_utc_millis(date_str, mode))
            })
            .collect::<Vec<Option<i64>>>(),
    );

    Arc::new(millis_array) as ArrayRef
}

#[test]
fn test_parse_datetime() {
    let res = parse_datetime("2020-05-16T09:30:00+05:00", DateParseMode::Utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {}", res);
    println!("utc_res: {}", utc_res);

    let res = parse_datetime("2020-05-16T09:30:00", DateParseMode::Utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {}", res);
    println!("utc_res: {}", utc_res);

    let res = parse_datetime("2020-05-16T09:30:00", DateParseMode::Local).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {}", res);
    println!("utc_res: {}", utc_res);

    let res = parse_datetime("2001/02/05 06:20", DateParseMode::Local).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {}", res);
    println!("utc_res: {}", utc_res);

    let res = parse_datetime("2001/02/05 06:20", DateParseMode::Utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {}", res);
    println!("utc_res: {}", utc_res);

    let res = parse_datetime("2000-01-01T08:00:00.000Z", DateParseMode::Utc).unwrap();
    let utc_res = res.with_timezone(&Utc);
    println!("res: {}", res);
    println!("utc_res: {}", utc_res);
}
