//! Scalar UDFs backing LogQL's parser stages.
//!
//! Each one maps `line: Utf8` → `labels: List<Struct<key, value>>`, so a `|
//! logfmt` or `| json` stage becomes a `Projection` that replaces the `labels`
//! column (DESIGN.md §3).

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListBuilder, StringBuilder, StructBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::table::{label_fields, labels_type};

/// `logfmt_parse(line) -> List<Struct<key, value>>`.
///
/// Implements bare `| logfmt` only: every key/value pair, no flags. Loki also
/// accepts `--strict`, `--keep-empty` and label expressions — see §5's
/// "Unsupported productions must say so".
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LogfmtParse {
    signature: Signature,
}

impl LogfmtParse {
    pub fn new() -> Self {
        Self {
            // Immutable: same line always yields the same labels, which lets the
            // optimizer treat calls as interchangeable and hoist constant ones.
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl Default for LogfmtParse {
    fn default() -> Self {
        Self::new()
    }
}

/// Wraps the impl so `plan.rs` can write `logfmt_parse().call(vec![col("line")])`.
pub fn logfmt_parse() -> ScalarUDF {
    ScalarUDF::from(LogfmtParse::new())
}

impl ScalarUDFImpl for LogfmtParse {
    fn name(&self) -> &str {
        "logfmt_parse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(labels_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // `to_array` normalises the Scalar case, so the loop below handles both a
        // column and a literal without branching.
        let lines = args.args[0].to_array(args.number_rows)?;
        let lines = lines.as_string::<i32>();

        let mut labels =
            ListBuilder::new(StructBuilder::from_fields(label_fields(), args.number_rows));

        for i in 0..lines.len() {
            if lines.is_null(i) {
                labels.append_null();
                continue;
            }

            let entries = labels.values();
            for (key, value) in parse_logfmt(lines.value(i)) {
                entries
                    .field_builder::<StringBuilder>(0)
                    .expect("field 0 is Utf8")
                    .append_value(key);
                entries
                    .field_builder::<StringBuilder>(1)
                    .expect("field 1 is Utf8")
                    .append_value(value);
                // A `StructBuilder` tracks its own length separately from its
                // children, so every row must be appended explicitly.
                entries.append(true);
            }
            labels.append(true);
        }

        Ok(ColumnarValue::Array(Arc::new(labels.finish()) as ArrayRef))
    }
}

/// Splits one logfmt line into its key/value pairs.
///
/// Borrowed from `line` rather than allocating: the caller copies into Arrow
/// buffers immediately, so no owned `String`s are needed.
fn parse_logfmt<'a>(line: &'a str) -> Vec<(&'a str, Cow<'a, str>)> {
    let mut labels = Vec::new();
    let mut remainder = line.trim_start();
    while let Some((key, value)) = next_pair(&mut remainder) {
        match value {
            Some(v) if !v.is_empty() => labels.push((key, v)),
            _ => continue,
        }
    }
    labels
}

fn next_pair<'a>(line: &mut &'a str) -> Option<(&'a str, Option<Cow<'a, str>>)> {
    if line.is_empty() {
        return None;
    }

    *line = line.trim_start();

    let key = next_key(line)?;
    if !eat(line, '=') {
        return Some((key, None));
    };
    let value = next_value(line);

    Some((key, value))
}

fn next_key<'a>(line: &mut &'a str) -> Option<&'a str> {
    let key_end = line.find(|c: char| c == '=' || c.is_whitespace())?;
    let key = &line[..key_end];
    *line = &line[key_end..];
    Some(key)
}

fn next_value<'a>(line: &mut &'a str) -> Option<Cow<'a, str>> {
    if eat(line, '"') {
        quoted_value(line)
    } else {
        unquoted_value(line)
    }
}

fn quoted_value<'a>(line: &mut &'a str) -> Option<Cow<'a, str>> {
    let mut escaped = false; // previous char was a backslash
    let mut saw_escape = false; // this value contains at least one

    for (i, c) in line.char_indices() {
        match c {
            _ if escaped => escaped = false,
            '\\' => {
                escaped = true;
                saw_escape = true;
            }
            '"' => {
                let (value, rest) = line.split_at(i);
                *line = &rest[1..]; // consume the closing quote
                return Some(if saw_escape {
                    Cow::Owned(unescape(value)?)
                } else {
                    Cow::Borrowed(value)
                });
            }
            _ => {}
        }
    }

    // Unterminated quote: take the rest, as non-strict logfmt does.
    Some(Cow::Borrowed(std::mem::take(line)))
}

/// Resolves the escapes Loki's `unquoteBytes` accepts. `None` on an unknown
/// escape, matching its `(nil, false)` — non-strict logfmt then drops the pair
/// rather than inventing a value.
fn unescape(value: &str) -> Option<String> {
    // The result is never longer than the input, so one allocation suffices.
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();

    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        out.push(match chars.next()? {
            '"' => '"',
            '\\' => '\\',
            '/' => '/',
            '\'' => '\'',
            'b' => '\u{8}',
            'f' => '\u{c}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'u' => {
                let hex: String = chars.by_ref().take(4).collect();
                char::from_u32(u32::from_str_radix(&hex, 16).ok()?)?
            }
            _ => return None,
        });
    }

    Some(out)
}

fn unquoted_value<'a>(line: &mut &'a str) -> Option<Cow<'a, str>> {
    let end = line.find(char::is_whitespace).unwrap_or(line.len());
    let (value, rest) = line.split_at(end);
    *line = rest;
    Some(Cow::Borrowed(value))
}

fn eat(line: &mut &str, c: char) -> bool {
    if line.starts_with(c) {
        *line = &line[1..];
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Borrows the pairs as `&str` so the expectations stay readable.
    ///
    /// Takes a reference rather than the `Vec` itself: a `Cow::Owned` value's
    /// `String` lives *in* that `Vec`, so the returned slices borrow from it and
    /// the caller has to keep it alive.
    fn as_strs<'a>(pairs: &'a [(&'a str, Cow<'a, str>)]) -> Vec<(&'a str, &'a str)> {
        pairs.iter().map(|(k, v)| (*k, v.as_ref())).collect()
    }

    #[test]
    fn parses_logfmt_lines() {
        let cases: [(&str, Vec<(&str, &str)>); 5] = [
            ("", vec![]),
            (
                "level=info msg=started",
                vec![("level", "info"), ("msg", "started")],
            ),
            (
                r#"level=error msg="something went wrong" code=500"#,
                vec![
                    ("level", "error"),
                    ("msg", "something went wrong"),
                    ("code", "500"),
                ],
            ),
            (
                "  level=info   msg=ok  ",
                vec![("level", "info"), ("msg", "ok")],
            ),
            (
                r#"level=info msg="\"ok""#,
                vec![("level", "info"), ("msg", r#""ok"#)],
            ),
        ];

        for (line, want) in cases {
            assert_eq!(as_strs(&parse_logfmt(line)), want, "line: {line:?}");
        }
    }

    /// Cases that reach past the happy path. Non-strict `| logfmt` never fails a
    /// line: it skips what it cannot read and keeps the rest.
    #[test]
    fn parses_malformed_logfmt_lines() {
        let cases: [(&str, Vec<(&str, &str)>); 8] = [
            // A key cannot contain whitespace, so `badkey` has no value and is
            // dropped — it must not swallow the following key.
            (
                "level=info badkey msg=ok",
                vec![("level", "info"), ("msg", "ok")],
            ),
            // Only the first `=` separates; the rest belongs to the value. And
            // the last pair must consume the cursor, or the leftover text gets
            // re-parsed into a phantom pair.
            ("msg=hello=world", vec![("msg", "hello=world")]), // An empty value is dropped exactly like a missing one — `parser.go`
            // has `if !l.keepEmpty && len(val) == 0 { continue }`. That is what
            // `--keep-empty` changes, and we do not support it.
            ("a= b=2", vec![("b", "2")]),
            ("a=1 b", vec![("a", "1")]),
            ("b a=1", vec![("a", "1")]),
            // An unterminated quote takes the rest of the line.
            (r#"msg="unterminated"#, vec![("msg", "unterminated")]),
            ("   ", vec![]),
            // Loki's decoder calls `unquoteBytes()`, so `\"` is unescaped rather
            // than kept literally.
            (
                r#"msg="say \"hi\"" code=1"#,
                vec![("msg", r#"say "hi""#), ("code", "1")],
            ),
        ];

        for (line, want) in cases {
            assert_eq!(as_strs(&parse_logfmt(line)), want, "line: {line:?}");
        }
    }
}
