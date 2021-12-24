use std::collections::HashSet;

lazy_static! {
    pub static ref BUILT_IN_SIGNALS: HashSet<&'static str> =
        vec!["width", "height", "padding", "autosize", "background"]
            .into_iter()
            .collect();

    pub static ref IMPLICIT_VARS: HashSet<&'static str> =
        vec!["datum", "event"]
            .into_iter()
            .collect();

    pub static ref ALL_DATA_FNS: HashSet<&'static str> = vec![
        "data", "indata", "vlSelectionTest", "vlSelectionResolve", "modify"
    ]
    .into_iter()
    .collect();

    pub static ref ALL_SCALE_FNS: HashSet<&'static str> = vec![
        "scale",
        "invert",
        "domain",
        "range",
        "bandwidth",
        "gradient",
    ]
    .into_iter()
    .collect();


    pub static ref SUPPORTED_DATA_FNS: HashSet<&'static str> = vec![
        "data", "vlSelectionTest", // "vlSelectionResolve"
    ]
    .into_iter()
    .collect();

    pub static ref SUPPORTED_SCALE_FNS: HashSet<&'static str> = Default::default();

    pub static ref SUPPORTED_EXPRESSION_FNS: HashSet<&'static str> = vec![
        // Math
        "abs", "acos", "asin", "atan", "ceil", "cos", "exp", "floor", "round", "sqrt", "tan",
        "log", "pow",

        // Type checking
        "isNaN", "isFinite", "isValid", "isDate",

        // Array
        "length", "span",

        // Datetime
        "year", "quarter", "month", "day", "date", "dayofyear", "hours", "minutes", "seconds",
        "milliseconds", "utcyear", "utcquarter", "utcmonth", "utcday", "utcdate", "utcdayofyear",
        "utchours", "utcminutes", "utcseconds", "utcmilliseconds", "datetime", "utc", "time",

        // Conversion
        "toBoolean", "toDate", "toNumber", "toString",

        // Control flow
        "if",
    ]
    .into_iter()
    .collect();
}

fn a() {
    // HashSet
}
