use chrono::{
    format::{parse, Parsed, StrftimeItems},
    {DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Timelike, Utc},
};
use regex::Regex;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};

lazy_static! {
    pub static ref ALL_STRF_DATETIME_ITEMS: Vec<StrftimeItems<'static>> = vec![
        // ISO 8601 / RFC 3339
        // e.g. 2001-07-08T00:34:60.026490+09:30
        StrftimeItems::new("%Y-%m-%dT%H:%M:%S%.f%:z"),

        // Like ISO 8601 with space instead of T
        // e.g. 2001-07-08 00:34:60.026490+09:30
        StrftimeItems::new("%Y-%m-%d %H:%M:%S%.f%:z"),

        // Like ISO 8601 with space, but forward slashes in date
        // e.g. 2001/07/08 00:34:60.026490+09:30
        StrftimeItems::new("%Y/%m/%d %H:%M:%S%.f%:z"),

        // month, day, year with slashes
        // e.g. 2001/07/08 00:34:60.026490+09:30
        StrftimeItems::new("%m/%d/%Y %H:%M:%S%.f%:z"),

        // ctime format
        // e.g. Sun Jul 8 00:34:60 2001
        StrftimeItems::new("%a %b %e %T %Y"),

        // e.g. 01 Jan 2012 00:00:00
        StrftimeItems::new("%d %b %Y %T"),

        // e.g. Sun, 01 Jan 2012 00:00:00
        StrftimeItems::new("%a, %d %b %Y %T"),

        // e.g. December 17, 1995 03:00:00
        StrftimeItems::new("%B %d, %Y %T"),
    ];

    pub static ref ALL_STRF_DATE_ITEMS: Vec<StrftimeItems<'static>> = vec![
        // // e.g. 1995/02/04
        // StrftimeItems::new("%Y/%m/%d"),

        // e.g. July 15, 2010
        StrftimeItems::new("%B %d, %Y"),

        // e.g. 01 Jan 2012
        StrftimeItems::new("%d %b %Y"),
    ];
}

pub fn parse_datetime(
    date_str: &str,
    default_input_tz: &Option<chrono_tz::Tz>,
) -> Option<DateTime<Utc>> {
    for strf_item in &*ALL_STRF_DATETIME_ITEMS {
        let mut parsed = Parsed::new();
        parse(&mut parsed, date_str, strf_item.clone()).ok();

        if let Ok(datetime) = parsed.to_datetime() {
            return Some(datetime.with_timezone(&chrono::Utc));
        } else if let (Ok(date), Ok(time)) = (parsed.to_naive_date(), parsed.to_naive_time()) {
            let datetime = NaiveDateTime::new(date, time);
            if date_str.ends_with('Z') {
                // UTC
                if let Some(datetime) = chrono::Utc.from_local_datetime(&datetime).earliest() {
                    return Some(datetime);
                }
            } else {
                // Local
                let local_tz = (*default_input_tz)?;
                let dt = if let Some(dt) = local_tz.from_local_datetime(&datetime).earliest() {
                    dt
                } else {
                    // Handle positive timezone transition by adding 1 hour
                    let datetime = datetime.with_hour(datetime.hour() + 1)?;
                    local_tz.from_local_datetime(&datetime).earliest()?
                };
                let dt_utc = dt.with_timezone(&chrono::Utc);
                return Some(dt_utc);
            }
        }
    }

    // Try plain dates
    if let Ok(date) = NaiveDate::parse_from_str(date_str, r#"%Y-%m-%d"#) {
        // UTC midnight to follow JavaScript convention
        let datetime = date.and_hms_opt(0, 0, 0).expect("Invalid date");
        return Some(chrono::Utc.from_utc_datetime(&datetime));
    } else {
        for strf_item in &*ALL_STRF_DATE_ITEMS {
            let mut parsed = Parsed::new();
            parse(&mut parsed, date_str, strf_item.clone()).ok();
            if let Ok(date) = parsed.to_naive_date() {
                // Local midnight to follow JavaScript convention
                let datetime = date.and_hms_opt(0, 0, 0).expect("Invalid date");
                let default_input_tz = (*default_input_tz)?;
                let datetime = default_input_tz.from_local_datetime(&datetime).earliest()?;
                return Some(datetime.with_timezone(&chrono::Utc));
            }
        }
    }

    parse_datetime_fallback(date_str, default_input_tz)
}

/// Parse a more generous specification of the iso 8601 date standard
/// Allow omission of time components
pub fn parse_datetime_fallback(
    date_str: &str,
    default_input_tz: &Option<chrono_tz::Tz>,
) -> Option<DateTime<Utc>> {
    let mut date_tokens = [String::from(""), String::from(""), String::from("")];
    let mut time_tokens = [
        String::from(""),
        String::from(""),
        String::from(""),
        String::from(""),
    ];
    let mut timezone_tokens = [String::from(""), String::from("")];
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
                } else if c.is_ascii_digit() {
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
                if c.is_ascii_digit() {
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
        if iso8601_date && !has_time {
            FixedOffset::east_opt(0).expect("FixedOffset::east out of bounds")
        } else {
            // Treat date as in local timezone
            let local_tz = (*default_input_tz)?;

            // No timezone specified, build NaiveDateTime
            let naive_date =
                NaiveDate::from_ymd_opt(year, month, day).expect("invalid or out-of-range date");
            let naive_time = NaiveTime::from_hms_milli_opt(hour, minute, second, milliseconds)
                .expect("invalid or out-of-range date");
            let naive_datetime = NaiveDateTime::new(naive_date, naive_time);

            local_tz
                .offset_from_local_datetime(&naive_datetime)
                .single()?
                .fix()
        }
    } else {
        let timezone_hours: i32 = timezone_tokens[0].parse().unwrap_or(0);
        let timezone_minutes: i32 = timezone_tokens[1].parse().unwrap_or(0);
        let time_offset_seconds = timezone_hours * 3600 + timezone_minutes * 60;

        if timezone_sign == '-' {
            FixedOffset::west_opt(time_offset_seconds).expect("FixedOffset::west out of bounds")
        } else {
            FixedOffset::east_opt(time_offset_seconds).expect("FixedOffset::east out of bounds")
        }
    };

    let parsed = offset
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .earliest()?
        .with_nanosecond(milliseconds * 1_000_000)?
        .with_timezone(&chrono::Utc);

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

pub fn parse_datetime_to_utc_millis(
    date_str: &str,
    default_input_tz: &Option<chrono_tz::Tz>,
) -> Option<i64> {
    // Parse to datetime
    let parsed_utc = parse_datetime(date_str, default_input_tz)?;

    // Extract milliseconds
    Some(parsed_utc.timestamp_millis())
}

pub fn datetime_strs_to_timestamp_millis(
    date_strs: &StringArray,
    default_input_tz: &Option<chrono_tz::Tz>,
) -> ArrayRef {
    let millis_array = TimestampMillisecondArray::from(
        date_strs
            .iter()
            .map(|date_str| -> Option<i64> {
                date_str
                    .and_then(|date_str| parse_datetime_to_utc_millis(date_str, default_input_tz))
            })
            .collect::<Vec<Option<i64>>>(),
    );

    Arc::new(millis_array) as ArrayRef
}
