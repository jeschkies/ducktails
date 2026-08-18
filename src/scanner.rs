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
            c if c.is_alphabetic() => self.identifier(),

            other => Err(ParseError::UnexpectedToken(other)),
        }
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
        todo!("implement string token")
    }

    pub fn identifier(&mut self) -> Result<Token, ParseError> {
        todo!("implement identifier token")
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
                matches!(got, Err(ParseError::UnexpectedToken(g)) if g == c),
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
                matches!(got, Err(ParseError::UnexpectedToken('!'))),
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
