//! Scalar UDFs backing LogQL's parser stages.
//!
//! Each one maps `line: Utf8` → `labels: List<Struct<key, value>>`, so a `|
//! logfmt` or `| json` stage becomes a `Projection` that replaces the `labels`
//! column (DESIGN.md §3).

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
fn parse_logfmt(line: &str) -> Vec<(&str, &str)> {
    let mut labels = Vec::new();
    let mut remainder = line.trim_start();
    while let Some(pair) = next_pair(&mut remainder) {
        labels.push(pair);
    }
    labels
}

fn next_pair<'a>(line: &mut &'a str) -> Option<(&'a str, &'a str)> {
    if line.is_empty() {
        return None;
    }

    *line = line.trim_start();

    let key = next_key(line)?;
    if !eat(line, '=') {
        return None;
    };
    let value = next_value(line)?;

    Some((key, value))
}

fn next_key<'a>(line: &mut &'a str) -> Option<&'a str> {
    let key_end = line.find('=')?;
    let key = &line[..key_end];
    *line = &line[key_end..];
    Some(key)
}

fn next_value<'a>(line: &mut &'a str) -> Option<&'a str> {
    let end = if eat(line, '"') {
        line.find('"')
    } else {
        line.find(char::is_whitespace)
    };

    if let Some(value_end) = end {
        let v = &line[..value_end];
        *line = &line[value_end..];
        eat(line, '"');
        Some(v)
    } else {
        Some(&line[..line.len()])
    }
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

    #[test]
    fn parses_logfmt_lines() {
        let cases: [(&str, Vec<(&str, &str)>); 4] = [
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
        ];

        for (line, want) in cases {
            assert_eq!(parse_logfmt(line), want, "line: {line:?}");
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
            ("msg=hello=world", vec![("msg", "hello=world")]),
            // An empty value is dropped exactly like a missing one — `parser.go`
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
            assert_eq!(parse_logfmt(line), want, "line: {line:?}");
        }
    }
}
