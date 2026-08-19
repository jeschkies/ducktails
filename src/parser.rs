use crate::error::ParseError;
use crate::scanner::{Scanner, Token};

/// How a matcher compares a label against its value.
///
/// A narrowed view of [`Token`] — only these four operators are legal inside a
/// selector, per `matcher` in Loki's `syntax.y`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `=`
    Eq,
    /// `!=`
    Neq,
    /// `=~`
    Re,
    /// `!~`
    Nre,
}

/// A single label matcher: `app="foo"`, `env=~"prod|staging"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

/// A stream selector: `{app="foo", env=~"prod|staging"}`.
///
/// The grammar admits `{}`; Loki rejects it semantically rather than at parse
/// time, so an empty `matchers` is representable here on purpose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selector {
    pub matchers: Vec<Matcher>,
}

/// The root of a LogQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// A log query: a selector, later plus a pipeline.
    Log(Selector),
    // Metric(SampleExpr) — milestone 2, see DESIGN.md §4.
}

pub struct Parser<'a> {
    scanner: Scanner<'a>,
}

impl<'a> Parser<'a> {
    pub fn parse(query: &str) -> Result<Expr, ParseError> {
        let mut p = Parser {
            scanner: Scanner::new(query),
        };
        let selector = p.selector()?;
        p.expect(Token::Eol)?;
        Ok(Expr::Log(selector))
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        let actual = self.scanner.next_token()?;
        if actual != expected {
            Err(ParseError::UnexpectedToken(actual, expected))
        } else {
            Ok(())
        }
    }

    fn eat(&mut self, expected: Token) -> Result<bool, ParseError> {
        let actual = self.scanner.peek_token()?;
        if actual == expected {
            // consume
            self.scanner.next_token()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Parse `{ name op "value", ... }`.
    fn selector(&mut self) -> Result<Selector, ParseError> {
        self.expect(Token::OpenBrace)?;

        let mut matchers = Vec::new();
        if self.scanner.peek_token()? != Token::CloseBrace {
            matchers.push(self.matcher()?);
            while self.eat(Token::Comma)? {
                matchers.push(self.matcher()?);
            }
        }

        self.expect(Token::CloseBrace)?;

        Ok(Selector { matchers })
    }

    fn matcher(&mut self) -> Result<Matcher, ParseError> {
        match self.scanner.next_token()? {
            Token::Identifier(name) => {
                let op = match self.scanner.next_token()? {
                    Token::Eq => MatchOp::Eq,
                    Token::Neq => MatchOp::Neq,
                    Token::Re => MatchOp::Re,
                    Token::Nre => MatchOp::Nre,
                    other => return Err(ParseError::UnexpectedToken(other, Token::Eq)), // TODO: !=, =~, !~
                };
                let value = match self.scanner.next_token()? {
                    Token::String(v) => v,
                    other => {
                        return Err(ParseError::UnexpectedToken(
                            other,
                            Token::String("string".into()),
                        ));
                    }
                };
                Ok(Matcher { name, op, value })
            }
            other => Err(ParseError::UnexpectedToken(
                other,
                Token::Identifier("Identifier".into()),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_selector() {
        let cases = [
            // The grammar admits `{}`; rejecting it is a semantic check, not a
            // parse error. See the note on `Selector`.
            ("{}", Expr::Log(Selector { matchers: vec![] })),
            (
                r#"{foo="bar"}"#,
                Expr::Log(Selector {
                    matchers: vec![Matcher {
                        name: "foo".into(),
                        op: MatchOp::Eq,
                        value: "bar".into(),
                    }],
                }),
            ),
            (
                r#"{foo="bar", bar!="baz"}"#,
                Expr::Log(Selector {
                    matchers: vec![
                        Matcher {
                            name: "foo".into(),
                            op: MatchOp::Eq,
                            value: "bar".into(),
                        },
                        Matcher {
                            name: "bar".into(),
                            op: MatchOp::Neq,
                            value: "baz".into(),
                        },
                    ],
                }),
            ),
            (
                r#"{foo=~"bar", bar!~"baz"}"#,
                Expr::Log(Selector {
                    matchers: vec![
                        Matcher {
                            name: "foo".into(),
                            op: MatchOp::Re,
                            value: "bar".into(),
                        },
                        Matcher {
                            name: "bar".into(),
                            op: MatchOp::Nre,
                            value: "baz".into(),
                        },
                    ],
                }),
            ),
        ];
        for (input, want) in cases {
            let got = Parser::parse(input).unwrap_or_else(|e| panic!("input {input:?}: {e}"));
            assert_eq!(got, want, "input: {input:?}");
        }
    }

    /// Every input here must fail, and fail for the stated reason.
    ///
    /// `ParseError` has no `PartialEq`, so a case cannot name the exact error
    /// value it wants. It names the error's *shape* instead, via a predicate.
    #[test]
    fn parser_selector_errors() {
        // `fn` pointers rather than closures: every closure has its own
        // anonymous type, so a table of closures would not typecheck as an array.
        type Check = fn(&ParseError) -> bool;

        let cases: [(&str, Check, &str); 10] = [
            (
                r#"{foo="bar",}"#,
                |e| matches!(e, ParseError::UnexpectedToken(Token::CloseBrace, _)),
                "trailing comma: the separator promises another matcher",
            ),
            (
                r#"{foo="bar" bar="baz"}"#,
                |e| matches!(e, ParseError::UnexpectedToken(Token::Identifier(n), Token::CloseBrace) if n == "bar"),
                "missing comma between matchers",
            ),
            (
                r#"{foo="bar"} garbage"#,
                |e| matches!(e, ParseError::UnexpectedToken(Token::Identifier(n), Token::Eol) if n == "garbage"),
                "trailing input after a complete query",
            ),
            (
                r#"{foo="bar""#,
                |e| {
                    matches!(
                        e,
                        ParseError::UnexpectedToken(Token::Eol, Token::CloseBrace)
                    )
                },
                "unterminated selector",
            ),
            (
                // `selector` only skips the matcher list when it peeks a `}`, so
                // here it commits to a matcher and reports the missing label
                // rather than the missing brace. Loki's goyacc agrees:
                // "unexpected $end, expecting IDENTIFIER".
                "{",
                |e| {
                    matches!(
                        e,
                        ParseError::UnexpectedToken(Token::Eol, Token::Identifier(_))
                    )
                },
                "lone opening brace",
            ),
            (
                r#"{="bar"}"#,
                |e| {
                    matches!(
                        e,
                        ParseError::UnexpectedToken(Token::Eq, Token::Identifier(_))
                    )
                },
                "matcher without a label name",
            ),
            (
                r#"{foo "bar"}"#,
                |e| matches!(e, ParseError::UnexpectedToken(Token::String(v), _) if v == "bar"),
                "matcher without an operator",
            ),
            (
                "{foo=bar}",
                |e| matches!(e, ParseError::UnexpectedToken(Token::Identifier(n), Token::String(_)) if n == "bar"),
                "unquoted matcher value",
            ),
            (
                r#"foo="bar""#,
                |e| matches!(e, ParseError::UnexpectedToken(Token::Identifier(n), Token::OpenBrace) if n == "foo"),
                "no opening brace: a query must start with a selector",
            ),
            (
                // Scanner-level failures must surface through the parser too.
                r#"{foo="bar}"#,
                |e| matches!(e, ParseError::UnexpectedEOL),
                "unterminated string literal",
            ),
        ];

        for (input, is_expected, why) in cases {
            match Parser::parse(input) {
                Ok(expr) => panic!("input {input:?} ({why}): expected an error, got {expr:?}"),
                Err(e) => assert!(is_expected(&e), "input {input:?} ({why}): got {e:?}"),
            }
        }
    }
}
