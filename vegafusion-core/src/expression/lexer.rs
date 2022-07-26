/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, ResultWithContext, VegaFusionError};
use regex::Regex;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Null,
    Bool { value: bool, raw: String },
    Number { value: f64, raw: String },
    String { value: String, raw: String },
    Identifier { value: String },
    Asterisk,
    CloseCurly,
    CloseParen,
    CloseSquare,
    Dot,
    DoubleEquals,
    TripleEquals,
    Exclamation,
    ExclamationEquals,
    ExclamationDoubleEquals,
    Minus,
    OpenCurly,
    OpenParen,
    OpenSquare,
    Plus,
    Slash,
    Percent,
    Comma,
    Colon,
    Question,
    GreaterThan,
    GreaterThanEquals,
    LessThan,
    LessThanEquals,
    LogicalAnd,
    LogicalOr,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Null => write!(f, "null"),
            Token::Bool { value: v, .. } => write!(f, "{}", v),
            Token::Number { value: v, .. } => write!(f, "{}", v),
            Token::String { value: v, .. } => write!(f, "\"{}\"", v),
            Token::Identifier { value: v } => write!(f, "{}", v),
            Token::Percent => write!(f, "%"),
            Token::Asterisk => write!(f, "*"),
            Token::CloseCurly => write!(f, "}}"),
            Token::CloseParen => write!(f, ")"),
            Token::CloseSquare => write!(f, "]"),
            Token::Dot => write!(f, "."),
            Token::DoubleEquals => write!(f, "=="),
            Token::TripleEquals => write!(f, "==="),
            Token::Minus => write!(f, "-"),
            Token::OpenCurly => write!(f, "{{"),
            Token::OpenParen => write!(f, "("),
            Token::OpenSquare => write!(f, "["),
            Token::Plus => write!(f, "+"),
            Token::Slash => write!(f, "/"),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Question => write!(f, "?"),
            Token::GreaterThan => write!(f, ">"),
            Token::LessThan => write!(f, "<"),
            Token::LessThanEquals => write!(f, "<="),
            Token::GreaterThanEquals => write!(f, ">="),
            Token::LogicalAnd => write!(f, "&&"),
            Token::LogicalOr => write!(f, "||"),
            Token::Exclamation => write!(f, "!"),
            Token::ExclamationEquals => write!(f, "!="),
            Token::ExclamationDoubleEquals => write!(f, "!=="),
        }
    }
}

/// Struct that maintains the current tokenization state
struct Tokenizer<'a> {
    current_index: usize,
    remaining_text: &'a str,
    full_text: &'a str,
}

impl<'a> Tokenizer<'a> {
    fn new(src: &str) -> Tokenizer {
        Tokenizer {
            current_index: 0,
            remaining_text: src,
            full_text: src,
        }
    }

    fn next_token(&mut self) -> Result<Option<(Token, usize, usize)>> {
        self.skip_whitespace();

        if self.remaining_text.is_empty() {
            Ok(None)
        } else {
            let start = self.current_index;
            let (tok, bytes_read) =
                tokenize_single_token(self.remaining_text).with_context(|| {
                    format!(
                        "Failed to parse token at position {} in expression: {}",
                        self.current_index, self.full_text
                    )
                })?;

            self.chomp(bytes_read);
            let end = self.current_index;

            Ok(Some((tok, start, end)))
        }
    }

    /// Advance to skip leading whitespace
    fn skip_whitespace(&mut self) {
        let skipped = leading_whitespace_len(self.remaining_text);
        self.chomp(skipped);
    }

    /// Advance current_index and update remaining text slice
    fn chomp(&mut self, num_bytes: usize) {
        self.remaining_text = &self.remaining_text[num_bytes..];
        self.current_index += num_bytes;
    }
}

/// function to perform tokanization
pub fn tokenize(src: &str) -> Result<Vec<(Token, usize, usize)>> {
    let mut tokenizer = Tokenizer::new(src);
    let mut tokens = Vec::new();

    while let Some(tok) = tokenizer.next_token()? {
        tokens.push(tok);
    }

    Ok(tokens)
}

/// Get the number of whitespace characters at the start of input string
fn leading_whitespace_len(data: &str) -> usize {
    match take_while(data, |ch| Ok(ch.is_whitespace())) {
        Ok(taken) => taken.len(),
        _ => 0,
    }
}

/// Consumes characters while a predicate evaluates to true, returning the &str that was taken
/// and its length
fn take_while<F>(data: &str, mut pred: F) -> Result<&str>
where
    F: FnMut(char) -> Result<bool>,
{
    let mut current_index = 0;
    for ch in data.chars() {
        let should_continue = pred(ch)?;

        if !should_continue {
            break;
        }
        current_index += ch.len_utf8();
    }
    if current_index == 0 {
        Err(VegaFusionError::parse("No matches for predicate"))
    } else {
        Ok(&data[..current_index])
    }
}

/// tokenizers
pub fn tokenize_single_token(data: &str) -> Result<(Token, usize)> {
    let next = match data.chars().next() {
        Some(c) => c,
        None => return Err(VegaFusionError::parse("Unexpected EOF")),
    };

    let (tok, length) = match next {
        '+' => tokenize_plus(data)?,
        '-' => tokenize_minus(data)?,
        '/' => tokenize_single_char_operator(data, '/', Token::Slash)?,
        '%' => tokenize_single_char_operator(data, '%', Token::Percent)?,
        '*' => tokenize_single_char_operator(data, '*', Token::Asterisk)?,
        '{' => (Token::OpenCurly, 1),
        '}' => (Token::CloseCurly, 1),
        '(' => (Token::OpenParen, 1),
        ')' => (Token::CloseParen, 1),
        '[' => (Token::OpenSquare, 1),
        ']' => (Token::CloseSquare, 1),
        ',' => (Token::Comma, 1),
        '?' => (Token::Question, 1),
        ':' => (Token::Colon, 1),
        '|' => tokenize_logical_or(data)?,
        '&' => tokenize_logical_and(data)?,
        c @ '"' | c @ '\'' => tokenize_string(data, c)?,
        '>' | '<' | '=' | '!' => tokenize_comparison_operators(data)?,
        c if c.is_ascii_digit() || c == '.' => tokenize_dot_or_number(data)?,
        c if c.is_alphabetic() || c == '_' => tokenize_ident(data)?,
        other => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid character: {}",
                other
            )))
        }
    };

    Ok((tok, length))
}

fn tokenize_plus(data: &str) -> Result<(Token, usize)> {
    let taken = take_while(data, |c| Ok(c == '+'))?;
    let token = match taken {
        "+" => Token::Plus,
        "++" => {
            return Err(VegaFusionError::parse(
                "Increment operator is not supported",
            ))
        }
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid number of consecutive + characters: {}",
                taken
            )))
        }
    };

    Ok((token, taken.len()))
}

fn tokenize_minus(data: &str) -> Result<(Token, usize)> {
    let taken = take_while(data, |c| Ok(c == '-'))?;
    let token = match taken {
        "-" => Token::Minus,
        "--" => {
            return Err(VegaFusionError::parse(
                "Decrement operator is not supported",
            ))
        }
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid number of consecutive - characters: {}",
                taken
            )))
        }
    };

    Ok((token, taken.len()))
}

/// Tokenize single character operators that may not be repeated without whitespace,
/// and which don't need special error message handling
fn tokenize_single_char_operator(data: &str, ch: char, token: Token) -> Result<(Token, usize)> {
    let taken = take_while(data, |c| Ok(c == ch))?;
    let token = match taken {
        c if c == ch.to_string() => token,
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid number of consecutive {} characters: {}",
                ch, taken
            )))
        }
    };

    Ok((token, taken.len()))
}

fn tokenize_logical_or(data: &str) -> Result<(Token, usize)> {
    let taken = take_while(data, |c| Ok(c == '|'))?;
    let token = match taken {
        "|" => return Err(VegaFusionError::parse("Bitwise OR operator not supported")),
        "||" => Token::LogicalOr,
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid number of consecutive | characters: {}",
                taken
            )))
        }
    };

    Ok((token, taken.len()))
}

fn tokenize_logical_and(data: &str) -> Result<(Token, usize)> {
    let taken = take_while(data, |c| Ok(c == '&'))?;
    let token = match taken {
        "&" => return Err(VegaFusionError::parse("Bitwise AND operator not supported")),
        "&&" => Token::LogicalAnd,
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid number of consecutive & characters: {}",
                taken
            )))
        }
    };

    Ok((token, taken.len()))
}

fn tokenize_comparison_operators(data: &str) -> Result<(Token, usize)> {
    let mut first_char = true;
    let taken = take_while(data, |c| {
        // Only keep ! when it's the first character so that consecutive ! characters are tokenized
        // separately (e.g. `!!!false` is valid, so is `true ==!false`)
        if first_char {
            first_char = false;
            Ok(c == '>' || c == '<' || c == '=' || c == '!')
        } else {
            Ok(c == '>' || c == '<' || c == '=')
        }
    })?;
    let token = match taken {
        ">" => Token::GreaterThan,
        "<" => Token::LessThan,
        ">=" => Token::GreaterThanEquals,
        "<=" => Token::LessThanEquals,
        "=" => {
            return Err(VegaFusionError::parse(
                "Assignment operator is not supported",
            ))
        }
        "==" => Token::DoubleEquals,
        "===" => Token::TripleEquals,
        "<<" => {
            return Err(VegaFusionError::parse(
                "Bitwise left shift operator not supported",
            ))
        }
        ">>" => {
            return Err(VegaFusionError::parse(
                "Bitwise signed right shift operator not supported",
            ))
        }
        ">>>" => {
            return Err(VegaFusionError::parse(
                "Bitwise right shift operator not supported",
            ))
        }
        "!" => Token::Exclamation,
        "!=" => Token::ExclamationEquals,
        "!==" => Token::ExclamationDoubleEquals,
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Invalid operator: {}",
                taken
            )))
        }
    };
    Ok((token, taken.len()))
}

fn unescape_unicode(data: &str, unicode_start_inds: &[usize]) -> String {
    // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String

    lazy_static! {
        // Of the form \xXX (note, the escape slash is gone at this point
        static ref UNICODE_RE_X2: Regex = Regex::new(r#"x[0-9a-fA-F]{2}"#).unwrap();

        // Of the form \uXXXX (note, the escape slash is gone at this point
        static ref UNICODE_RE_U4: Regex = Regex::new(r#"u[0-9a-fA-F]{4}"#).unwrap();

        // Of the form \u{X} through \u{XXXXXX} (note, the escape slash is gone at this point
        static ref UNICODE_RE_BRACKET_U6: Regex = Regex::new(r#"u\{[0-9a-fA-F]{1,6}}"#).unwrap();
    }

    let mut result = String::from(data);

    // Iterate over unicode start indices in reverse order so that indices are still valid as the
    // result string is mutated
    for i in unicode_start_inds.iter().rev() {
        let (hex_str, start, end) = if let Some(mat) = UNICODE_RE_X2.find_at(&result, *i) {
            let slice = &data[mat.start() + 1..mat.end()];
            (slice, mat.start(), mat.end())
        } else if let Some(mat) = UNICODE_RE_U4.find_at(&result, *i) {
            let slice = &data[mat.start() + 1..mat.end()];
            (slice, mat.start(), mat.end())
        } else if let Some(mat) = UNICODE_RE_BRACKET_U6.find_at(&result, *i) {
            // Make sure to not include curly brackets!
            let slice = &data[mat.start() + 2..mat.end() - 1];
            (slice, mat.start(), mat.end())
        } else {
            // ignore it
            continue;
        };

        // Regex ensures this won't fail
        let unicode_int = u32::from_str_radix(hex_str, 16).unwrap();
        if let Some(unicode_val) = std::char::from_u32(unicode_int) {
            result.replace_range(start..end, &unicode_val.to_string());
        }
    }

    result
}

fn tokenize_string(data: &str, quote_char: char) -> Result<(Token, usize)> {
    let mut in_escape = false;
    let mut in_string = false;

    let mut unescaped: String = String::new();
    let mut unicode_start_inds: Vec<usize> = Vec::new();

    let taken = take_while(data, |c| {
        if !in_string {
            in_string = c == quote_char;
            // Return false shouldn't happen, but if it does, take_while will bail
            Ok(in_string)
        } else if in_escape {
            // Unescape character
            // See https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String
            match c {
                '0' => unescaped.push('\u{0000}'), // null character
                '\'' => unescaped.push('\''),      // single quote
                '"' => unescaped.push('\"'),       // double quote
                '\\' => unescaped.push('\\'),      // backslash
                'n' => unescaped.push('\n'),       // newline
                '\n' => {}                         // used to escape multiline string
                'r' => unescaped.push('\r'),       // carriage return
                'v' => unescaped.push('\u{000B}'), // vertical tab
                't' => unescaped.push('\t'),       // tab
                'b' => unescaped.push('\u{0008}'), // backspace
                'f' => unescaped.push('\u{000C}'), // form feed
                'u' => {
                    // Remove slash, keep u, and record index as the start of a unicode sequence
                    unicode_start_inds.push(unescaped.len());
                    unescaped.push('u');
                }
                'x' => {
                    // Remove slash, keep x, and record index as the start of a unicode sequence
                    unicode_start_inds.push(unescaped.len());
                    unescaped.push('x')
                }
                _ => {
                    // Invalid escape character sequence, JavaScript just removes slash
                    unescaped.push(c)
                }
            };
            in_escape = false;
            Ok(true)
        } else if c == quote_char {
            // end of string
            in_string = false;
            Ok(false)
        } else {
            in_escape = c == '\\';
            if !in_escape {
                // c is regular, non-escaped, character
                unescaped.push(c);
            }
            Ok(true)
        }
    })?;

    if in_string {
        Err(VegaFusionError::parse(
            "Expression ends with unterminated string",
        ))
    } else {
        // Build raw string by adding closing quote character
        let mut raw = taken.to_string();
        raw.push(quote_char);
        let bytes_read = raw.len();
        let token = Token::String {
            value: unescape_unicode(&unescaped, &unicode_start_inds),
            raw,
        };

        // Add 1 to bytes_read because we stopped reading on the closing quote
        Ok((token, bytes_read))
    }
}

fn tokenize_dot_or_number(data: &str) -> Result<(Token, usize)> {
    let mut seen_digit = false;
    let mut seen_dot = false;
    let mut seen_e = false;
    let mut last_char_was_e = false;

    let taken = take_while(data, |c| {
        if c.is_ascii_digit() {
            seen_digit = true;
            last_char_was_e = false;
            Ok(true)
        } else if c == '.' && !seen_dot && !seen_e {
            seen_dot = true;
            Ok(true)
        } else if (c == 'e' || c == 'E') && seen_digit && !seen_e {
            seen_e = true;
            last_char_was_e = true;
            Ok(true)
        } else if (c == '+' || c == '-') && last_char_was_e {
            last_char_was_e = false;
            Ok(true)
        } else {
            Ok(false)
        }
    })?;

    if seen_dot {
        if taken == "." {
            // It was just a dot, not a float
            Ok((Token::Dot, 1))
        } else {
            // Check for leading zeros. Only allowed right before decimal point (as in 0.5)
            if &taken[..2] == "00" || &taken[..1] == "0" && &taken[..2] != "0." {
                return Err(VegaFusionError::parse("Floats may not have leading zeros"));
            }
            let n: f64 = taken.parse()?;
            Ok((
                Token::Number {
                    value: n,
                    raw: taken.to_string(),
                },
                taken.len(),
            ))
        }
    } else {
        if &taken[..1] == "0" && taken.len() > 1 {
            return Err(VegaFusionError::parse(
                "Integers may not have leading zeros",
            ));
        }
        let n: f64 = taken.parse()?;
        Ok((
            Token::Number {
                value: n,
                raw: taken.to_string(),
            },
            taken.len(),
        ))
    }
}

fn tokenize_ident(data: &str) -> Result<(Token, usize)> {
    let taken = take_while(data, |ch| Ok(ch == '_' || ch.is_alphanumeric()))
        .with_context(|| "Failed to tokenize identifier".to_string())?;

    let tok = match taken {
        "true" => Token::Bool {
            value: true,
            raw: taken.to_string(),
        },
        "false" => Token::Bool {
            value: false,
            raw: taken.to_string(),
        },
        "null" => Token::Null,
        _ => Token::Identifier {
            value: taken.to_string(),
        },
    };

    Ok((tok, taken.len()))
}

#[cfg(test)]
mod test_tokenizers {
    use crate::expression::lexer::tokenize;

    // Note: We test at the parse-to-AST level, not the tokenization level. These try_* tests are
    // here for experimentation, not test coverage
    #[test]
    fn try_tokenize() {
        let res = tokenize(" { } ").unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(" + ").unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(" -() ").unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(" * ").unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(" || && ").unwrap();
        println!("tokenized: {:?}", res);
    }

    #[test]
    fn try_tokenizer_unicode_str() {
        let res = tokenize(r#" "Hello, world" "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 'Hello, world \xAE \u2665 \u{1F602}' "#).unwrap();
        println!("tokenized: {:?}", res);
    }

    #[test]
    fn try_tokenize_dot_or_number() {
        let res = tokenize(r#" 23.500 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" .500 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 5. "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 500 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 5e7 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 5e-7 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 5e+7 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 5.e7 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 5.e+7 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" .3e-7 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" .3e-007 "#).unwrap();
        println!("tokenized: {:?}", res);

        // Don't get confused when no digit is present
        let res = tokenize(r#" .e23 "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize(r#" 'asdf'.e "#).unwrap();
        println!("tokenized: {:?}", res);

        let res = tokenize("  foo*.345 ").unwrap();
        println!("tokenized: {:?}", res);
    }
}
