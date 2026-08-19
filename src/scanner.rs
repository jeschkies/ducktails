use std::fmt;

use crate::error::ParseError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Comma,
    Dot,
    OpenBrace,
    CloseBrace,
    Eq,
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
        match self.advance().ok_or(ParseError::UnexpectedEOL)? {
            '{' => Ok(Token::OpenBrace),
            '}' => Ok(Token::CloseBrace),
            ',' => Ok(Token::Comma),
            '.' => Ok(Token::Dot),
            '(' => Ok(Token::OpenParenthesis),
            ')' => Ok(Token::CloseParenthesis),

            '=' if self.eat('~') => Ok(Token::Re), // =~ before =
            '=' => Ok(Token::Eq),

            '!' if self.eat('=') => Ok(Token::Neq),
            '!' if self.eat('~') => Ok(Token::Nre),
            '!' if self.eat('>') => Ok(Token::Npa), // !> negated pattern

            '|' if self.eat('=') => Ok(Token::PipeExact),
            '|' if self.eat('~') => Ok(Token::PipeMatch),
            '|' if self.eat('>') => Ok(Token::PipePattern),
            '|' => Ok(Token::Pipe),

            '"' => self.string(),
            c if c.is_ascii_alphabetic() || c == ':' || c == '_' => self.identifier(c),

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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scan until the input is exhausted. Treats `UnexpectedEOL` as the terminator
    /// and propagates any other error.
    fn scan_all(input: &str) -> Result<Vec<Token>, ParseError> {
        let mut scanner = Scanner::new(input);
        let mut out = Vec::new();
        loop {
            match scanner.next_token() {
                Ok(token) => out.push(token),
                Err(ParseError::UnexpectedEOL) => return Ok(out),
                Err(other) => return Err(other),
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
        let got = Scanner::new("").next_token();
        assert!(matches!(got, Err(ParseError::UnexpectedEOL)), "got {got:?}");
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

    #[test]
    fn skips_whitespace_between_tokens() {
        // LogQL is whitespace-insensitive between tokens. See TODO(human) in next_token.
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
