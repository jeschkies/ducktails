use std::fmt;
use std::time::Duration;

use bytesize::ByteSize;

use crate::error::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Comma,
    Dot,
    OpenBrace,
    CloseBrace,
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Neq,
    Re,
    Nre,
    Npa,
    PipeExact,
    PipeMatch,
    PipePattern,
    Pipe,
    OpenParenthesis,
    CloseParenthesis,

    Identifier(String),
    String(String),

    Number(f64),
    Duration(Duration),
    Bytes(u64),

    Eol,
}

/// Renders a token as it appears in source, so error messages can quote the
/// user's own syntax back at them. Deliberately exhaustive: adding a `Token`
/// variant should fail to compile until it is spelled here too.
impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Comma => f.write_str(","),
            Token::Dot => f.write_str("."),
            Token::OpenBrace => f.write_str("{"),
            Token::CloseBrace => f.write_str("}"),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("!="),
            Token::Gt => f.write_str(">"),
            Token::Gte => f.write_str(">="),
            Token::Lt => f.write_str("<"),
            Token::Lte => f.write_str("<="),
            Token::Re => f.write_str("=~"),
            Token::Nre => f.write_str("!~"),
            Token::Npa => f.write_str("!>"),
            Token::PipeExact => f.write_str("|="),
            Token::PipeMatch => f.write_str("|~"),
            Token::PipePattern => f.write_str("|>"),
            Token::Pipe => f.write_str("|"),
            Token::OpenParenthesis => f.write_str("("),
            Token::CloseParenthesis => f.write_str(")"),

            Token::Identifier(name) => f.write_str(name),
            // `{:?}` on a str re-adds the quotes and escapes, which is close
            // enough to LogQL's Go-style string syntax.
            Token::String(value) => write!(f, "{value:?}"),

            Token::Number(value) => write!(f, "{value}"),
            // `{:?}` on a `Duration` prints Go-ish units (`5400s`, `1.5s`), so a
            // message quotes something the user could paste back.
            Token::Duration(value) => write!(f, "{value:?}"),
            // The unit the user wrote is not kept, so this re-renders in bytes:
            // exact and still valid LogQL, if not what they typed.
            Token::Bytes(value) => write!(f, "{value}B"),

            Token::Eol => write!(f, "EOL"),
        }
    }
}

pub struct Scanner<'a> {
    source: &'a str,
}

impl<'a> Scanner<'a> {
    pub fn new(query: &'a str) -> Self {
        Scanner { source: query }
    }

    pub fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_trivia();
        let Some(next_char) = self.advance() else {
            return Ok(Token::Eol);
        };
        match next_char {
            '{' => Ok(Token::OpenBrace),
            '}' => Ok(Token::CloseBrace),
            ',' => Ok(Token::Comma),
            '.' => Ok(Token::Dot),
            '(' => Ok(Token::OpenParenthesis),
            ')' => Ok(Token::CloseParenthesis),

            '=' if self.eat('~') => Ok(Token::Re), // =~ before =
            '=' => Ok(Token::Eq),

            '>' if self.eat('=') => Ok(Token::Gte),
            '>' => Ok(Token::Gt),
            '<' if self.eat('=') => Ok(Token::Lte),
            '<' => Ok(Token::Lt),

            '!' if self.eat('=') => Ok(Token::Neq),
            '!' if self.eat('~') => Ok(Token::Nre),
            '!' if self.eat('>') => Ok(Token::Npa), // !> negated pattern

            '|' if self.eat('=') => Ok(Token::PipeExact),
            '|' if self.eat('~') => Ok(Token::PipeMatch),
            '|' if self.eat('>') => Ok(Token::PipePattern),
            '|' => Ok(Token::Pipe),

            '"' => self.string(),
            c if c.is_ascii_alphabetic() || c == ':' || c == '_' => self.identifier(c),
            i if i.is_ascii_digit() => self.number(i),

            other => Err(ParseError::UnexpectedChar(other)),
        }
    }

    pub fn peek_token(&mut self) -> Result<Token, ParseError> {
        let saved = self.source;
        let result = self.next_token();
        self.source = saved; // rewind
        result
    }

    fn skip_trivia(&mut self) {
        self.source = self.source.trim_start();
    }

    pub fn advance(&mut self) -> Option<char> {
        let mut chars = self.source.chars();
        let c = chars.next()?;
        self.source = chars.as_str();
        Some(c)
    }

    pub fn peek(&self) -> Option<char> {
        self.source.chars().next()
    }

    pub fn eat(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    pub fn string(&mut self) -> Result<Token, ParseError> {
        let mut value = String::new();
        loop {
            match self.advance().ok_or(ParseError::UnexpectedEOL)? {
                '"' => return Ok(Token::String(value)),
                '\\' => {
                    let esc = self.advance().ok_or(ParseError::UnexpectedEOL)?;
                    value.push(match esc {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '\\' => '\\',
                        '"' => '"',
                        other => other,
                    });
                }
                other => value.push(other),
            }
        }
    }

    fn is_label_cont(c: char) -> bool {
        c.is_ascii_alphanumeric() || c == '_' || c == ':'
    }

    pub fn identifier(&mut self, first: char) -> Result<Token, ParseError> {
        let end = self
            .source
            .find(|c: char| !Scanner::is_label_cont(c))
            .unwrap_or(self.source.len()); // all remaining chars are valid
        let (rest, tail) = self.source.split_at(end);
        self.source = tail;

        let mut name = String::with_capacity(1 + rest.len());
        name.push(first);
        name.push_str(rest);
        Ok(Token::Identifier(name))
    }

    /// The runes a byte size may be spelled with: B kB MB GB TB PB KB KiB MiB
    /// GiB TiB PiB.
    ///
    /// Deliberately NOT E/Z/Y: Loki excludes them because the value may not fit
    /// u64. Lowercase is excluded too (bar `k` and `i`), matching Loki — so
    /// `10gb` is not a byte size, and scans as `10` followed by `gb`.
    fn is_bytes_size_rune(c: char) -> bool {
        matches!(c, 'B' | 'i' | 'k' | 'K' | 'M' | 'G' | 'T' | 'P')
    }

    /// The runes a duration may be spelled with: ns, us (or µs), ms, s, m, h,
    /// d, w, y.
    fn is_duration_rune(c: char) -> bool {
        matches!(c, 'n' | 'u' | 'µ' | 'm' | 's' | 'h' | 'd' | 'w' | 'y')
    }

    /// Digits and the unit runes, i.e. everything one numeric token can contain.
    ///
    /// A single run of these covers `1h30m` as well as `3GiB`, so the token's
    /// extent is found before anything tries to interpret it.
    fn is_number_cont(c: char) -> bool {
        c.is_ascii_digit()
            || c == '.'
            || Scanner::is_duration_rune(c)
            || Scanner::is_bytes_size_rune(c)
    }

    /// Scans a number with an optional unit: `10`, `1.5`, `1h30m`, `3GiB`.
    ///
    /// Duration before bytes, as Loki's lexer does — which is why `10m` is ten
    /// minutes rather than ten megabytes. A bare number stays a number: Loki's
    /// `tryScanBytes` needs at least one unit rune, whereas `ByteSize` would
    /// happily read `500` as 500 bytes.
    pub fn number(&mut self, first: char) -> Result<Token, ParseError> {
        let end = self
            .source
            .find(|c: char| !Scanner::is_number_cont(c))
            .unwrap_or(self.source.len()); // all remaining chars are valid
        let (rest, tail) = self.source.split_at(end);
        self.source = tail;

        let mut literal = String::with_capacity(1 + rest.len());
        literal.push(first);
        literal.push_str(rest);

        if literal.chars().all(|c| c.is_ascii_digit() || c == '.') {
            return literal
                .parse()
                .map(Token::Number)
                .map_err(|_| ParseError::Unsupported);
        }
        if let Some(duration) = parse_duration(&literal) {
            return Ok(Token::Duration(duration));
        }
        literal
            .parse::<ByteSize>()
            .map(|size| Token::Bytes(size.as_u64()))
            .map_err(|_| ParseError::Unsupported)
    }
}

/// One duration unit and its length in nanoseconds.
///
/// Ordered so that a two-character unit is tried before the one-character unit
/// it starts with: `ms` must not read as `m` with a stray `s` behind it.
const DURATION_UNITS: [(&str, u128); 10] = [
    ("ns", 1),
    ("us", 1_000),
    ("µs", 1_000),
    ("ms", 1_000_000),
    ("s", 1_000_000_000),
    ("m", 60 * 1_000_000_000),
    ("h", 60 * 60 * 1_000_000_000),
    // Prometheus' calendar-free definitions, which is what Loki inherits.
    ("d", 24 * 60 * 60 * 1_000_000_000),
    ("w", 7 * 24 * 60 * 60 * 1_000_000_000),
    ("y", 365 * 24 * 60 * 60 * 1_000_000_000),
];

/// Parses a LogQL duration: `2h`, `1h30m`, `1.5s`, `7d`.
///
/// Loki tries Prometheus' `model.ParseDuration` first and falls back to Go's
/// `time.ParseDuration`, so the accepted set is the union of the two: Go's
/// `ns`/`us`/`µs`/`ms`/`s`/`m`/`h` with fractions, plus Prometheus' `d`, `w` and
/// `y`. Unlike Prometheus the units need not descend — Go does not require it
/// either, and rejecting `30m1h` buys the grammar nothing.
///
/// `None` rather than an error: the caller still has byte sizes to try, and only
/// it knows that both have failed.
fn parse_duration(literal: &str) -> Option<Duration> {
    let mut rest = literal;
    let mut nanos: u128 = 0;

    while !rest.is_empty() {
        // `None` means the magnitude runs to the end of the literal, i.e. a
        // trailing number with no unit — `1h30`.
        let magnitude_end = rest.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
        let (magnitude, tail) = rest.split_at(magnitude_end);
        let magnitude: f64 = magnitude.parse().ok()?;

        let (unit, tail) = DURATION_UNITS
            .iter()
            .find(|(name, _)| tail.starts_with(name))
            .map(|(name, unit)| (unit, &tail[name.len()..]))?;

        // Rounds to the nearest nanosecond, as Go's parser does. `as u128`
        // saturates rather than wrapping, so an absurd magnitude fails below
        // instead of silently becoming small.
        let unit_nanos = magnitude * *unit as f64;
        if !unit_nanos.is_finite() {
            return None;
        }
        nanos = nanos.checked_add(unit_nanos.round() as u128)?;
        rest = tail;
    }

    Some(Duration::from_nanos(u64::try_from(nanos).ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scan until the input is exhausted. `Token::EOL` terminates the stream and
    /// is not included in the output; any error propagates.
    fn scan_all(input: &str) -> Result<Vec<Token>, ParseError> {
        let mut scanner = Scanner::new(input);
        let mut out = Vec::new();
        loop {
            match scanner.next_token()? {
                Token::Eol => return Ok(out),
                token => out.push(token),
            }
        }
    }

    #[test]
    fn scans_every_punctuation_token() {
        let cases = [
            ("{", Token::OpenBrace),
            ("}", Token::CloseBrace),
            (",", Token::Comma),
            (".", Token::Dot),
            ("(", Token::OpenParenthesis),
            (")", Token::CloseParenthesis),
            ("=", Token::Eq),
            ("!=", Token::Neq),
            ("=~", Token::Re),
            ("!~", Token::Nre),
            ("!>", Token::Npa),
            ("|=", Token::PipeExact),
            ("|~", Token::PipeMatch),
            ("|>", Token::PipePattern),
            ("|", Token::Pipe),
            ("\"foo\"", Token::String("foo".into())),
        ];
        for (input, want) in cases {
            let got = Scanner::new(input)
                .next_token()
                .unwrap_or_else(|e| panic!("input {input:?}: {e}"));
            assert_eq!(got, want, "input: {input:?}");
        }
    }

    #[test]
    fn advances_across_a_token_sequence() {
        let got = scan_all("{},.()").expect("should scan");
        assert_eq!(
            got,
            vec![
                Token::OpenBrace,
                Token::CloseBrace,
                Token::Comma,
                Token::Dot,
                Token::OpenParenthesis,
                Token::CloseParenthesis,
            ]
        );
    }

    #[test]
    fn two_char_operators_consume_both_chars() {
        // If `eat` did not advance, `=~` would scan as Eq followed by a stray `~`.
        let got = scan_all("=~|=!>").expect("should scan");
        assert_eq!(got, vec![Token::Re, Token::PipeExact, Token::Npa]);
    }

    #[test]
    fn failed_eat_does_not_consume() {
        // The three `|` guards all fail here, so `,` must still be available.
        let got = scan_all("|,").expect("should scan");
        assert_eq!(got, vec![Token::Pipe, Token::Comma]);
    }

    #[test]
    fn empty_input_signals_end() {
        // End of input is a token, not an error, so the parser can `expect` it.
        let got = Scanner::new("").next_token();
        assert!(matches!(got, Ok(Token::Eol)), "got {got:?}");
    }

    #[test]
    fn rejects_unknown_characters() {
        // Deliberately excludes letters, digits and `#` — those become identifiers,
        // numbers and comments once the scanner grows.
        for c in ['@', '$', ';', '?'] {
            let input = c.to_string();
            let got = Scanner::new(&input).next_token();
            assert!(
                matches!(got, Err(ParseError::UnexpectedChar(g)) if g == c),
                "input {input:?}: got {got:?}"
            );
        }
    }

    #[test]
    fn bare_bang_is_an_error() {
        // All three `!` guards fail without consuming, so the match falls through
        // to `other` and reports the `!` itself.
        for input in ["!", "!x"] {
            let got = Scanner::new(input).next_token();
            assert!(
                matches!(got, Err(ParseError::UnexpectedChar('!'))),
                "input {input:?}: got {got:?}"
            );
        }
    }

    /// The scanner resolves units itself, so the parser never sees `"10"` plus a
    /// suffix — it gets a value it can put straight into a `LabelValue`.
    #[test]
    fn scans_numbers_durations_and_byte_sizes() {
        let cases = [
            // No unit: a plain number, even though `ByteSize` would read `500`
            // as 500 bytes.
            ("500", Token::Number(500.0)),
            ("0.5", Token::Number(0.5)),
            ("2h", Token::Duration(Duration::from_secs(2 * 60 * 60))),
            // One token, not `1h` followed by `30m`.
            ("1h30m", Token::Duration(Duration::from_secs(90 * 60))),
            ("1.5s", Token::Duration(Duration::from_millis(1_500))),
            ("500ms", Token::Duration(Duration::from_millis(500))),
            ("500us", Token::Duration(Duration::from_micros(500))),
            ("500µs", Token::Duration(Duration::from_micros(500))),
            // Prometheus' units, which Go's own parser does not take.
            ("7d", Token::Duration(Duration::from_secs(7 * 24 * 60 * 60))),
            (
                "1y",
                Token::Duration(Duration::from_secs(365 * 24 * 60 * 60)),
            ),
            // `ByteSize` reads a lone `m` as megabytes, so trying duration first
            // is what makes this ten minutes.
            ("10m", Token::Duration(Duration::from_secs(10 * 60))),
            // SI vs. binary, as `humanize.ParseBytes` separates them.
            ("10GB", Token::Bytes(10_000_000_000)),
            ("3GiB", Token::Bytes(3 * 1024 * 1024 * 1024)),
            // Truncates to 0 — the edge case `bytesize` was chosen for.
            ("0.5B", Token::Bytes(0)),
            // `M` is a byte rune but not a duration one, so case alone decides.
            ("1M", Token::Bytes(1_000_000)),
        ];
        for (input, want) in cases {
            let got = Scanner::new(input)
                .next_token()
                .unwrap_or_else(|e| panic!("input {input:?}: {e}"));
            assert_eq!(got, want, "input: {input:?}");
        }
    }

    #[test]
    fn rejects_numbers_it_cannot_resolve() {
        // A unit that is neither a duration nor a byte size, a magnitude that is
        // not a number at all, and a trailing magnitude with no unit of its own.
        for input in ["10KiBs", "1.2.3", "5ns7"] {
            let got = Scanner::new(input).next_token();
            assert!(
                matches!(got, Err(ParseError::Unsupported)),
                "input {input:?}: got {got:?}"
            );
        }
    }

    /// `1e3KB` is an error in Loki rather than 1000000 — see the `bytesize` note
    /// in `Cargo.toml`. `e` is in neither unit class, so the numeric token ends
    /// at `1` and the rest scans as an identifier, which no production accepts.
    #[test]
    fn an_exponent_does_not_scan_as_a_byte_size() {
        let got = scan_all("1e3KB").expect("should scan");
        assert_eq!(
            got,
            vec![Token::Number(1.0), Token::Identifier("e3KB".into())]
        );
    }

    #[test]
    fn skips_whitespace_between_tokens() {
        // LogQL is whitespace-insensitive between tokens; `skip_trivia` handles it.
        let got = scan_all("{ } , ( )").expect("should scan");
        assert_eq!(
            got,
            vec![
                Token::OpenBrace,
                Token::CloseBrace,
                Token::Comma,
                Token::OpenParenthesis,
                Token::CloseParenthesis,
            ]
        );
    }
}
