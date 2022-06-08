pub fn escape_field(col: &str) -> String {
    // Escape single quote with backslash
    let col = col.replace('\'', "\\'");

    // Escape double quote with backslash
    let col = col.replace('\"', "\\\"");

    // Escape period with backslash

    col.replace('.', "\\.")
}

pub fn unescape_field(col: &str) -> String {
    // Unescape backslash single quote
    let col = col.replace("\\'", "'");

    // Unescape backslash double quote
    let col = col.replace("\\\"", "\"");

    //  Unescape backslash period

    col.replace("\\.", ".")
}

#[cfg(test)]
mod tests {
    use crate::expression::escape::{escape_field, unescape_field};

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
