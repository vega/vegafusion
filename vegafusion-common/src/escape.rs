pub fn escape_field(col: &str) -> String {
    // Escape single quote, double quote, period, and brackets with a backslash
    col.replace('\'', "\\'")
        .replace('\"', "\\\"")
        .replace('.', "\\.")
        .replace('[', "\\[")
        .replace(']', "\\]")
}

pub fn unescape_field(col: &str) -> String {
    // Unescape single quote, double quote, period, and brackets
    col.replace("\\'", "'")
        .replace("\\\"", "\"")
        .replace("\\.", ".")
        .replace("\\[", "[")
        .replace("\\]", "]")
}

#[cfg(test)]
mod tests {
    use crate::escape::{escape_field, unescape_field};

    #[test]
    fn test_escape() {
        let col = "'foo'_._\"bar\"";
        let escaped = escape_field(col);
        assert_eq!(escaped, r#"\'foo\'_\._\"bar\""#)
    }

    #[test]
    fn test_unescape() {
        let col = r#"\'foo\'_\._\"bar\""#;
        let unescaped = unescape_field(col);
        assert_eq!(unescaped, "'foo'_._\"bar\"")
    }
}
