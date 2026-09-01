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

/// `|= "error"`, `!~ "debug|trace"`. Distinct from `MatchOp`: `=` on a label is
/// equality, `|=` on a line is *containment*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineFilterOp {
    Contains,
    NotContains,
    Re,
    Nre,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineFilter {
    pub op: LineFilterOp,
    pub value: String,
}

/// One stage of a log pipeline. §3 models a pipeline as an ordered sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    Line(LineFilter),
    Logfmt,
} // Json / LabelFilter later

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogQuery {
    pub selector: Selector,
    pub pipeline: Vec<Stage>,
}

/// The root of a LogQL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// A log query: a selector, later plus a pipeline.
    Log(LogQuery),
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
        let log_query = p.log_query()?;
        p.expect(Token::Eol)?;
        Ok(Expr::Log(log_query))
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

    /// Parse `{ name op "value", ... } |= "Debug"`.
    fn log_query(&mut self) -> Result<LogQuery, ParseError> {
        let selector = self.selector()?;
        let pipeline = self.pipeline()?;

        Ok(LogQuery { selector, pipeline })
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

    /// Parse `|= "Debug" | Json !~ "Fatal|Info"`.
    fn pipeline(&mut self) -> Result<Vec<Stage>, ParseError> {
        // TODO: maybe define iterator and use collect
        let mut stages: Vec<Stage> = vec![];
        while let Some(next_stage) = self.stage()? {
            stages.push(next_stage);
        }

        Ok(stages)
    }

    /// Parse `|= "Debug"` or `| Json` or `!~ "Fatal|Info"` etc.
    fn stage(&mut self) -> Result<Option<Stage>, ParseError> {
        match self.scanner.peek_token()? {
            Token::Pipe => {
                self.scanner.next_token()?; // eat previous pipe token
                match self.scanner.next_token()? {
                    Token::Identifier(id) if id == "logfmt" => Ok(Some(Stage::Logfmt)),
                    other => Err(ParseError::UnexpectedToken(
                        other,
                        Token::Identifier("identifier".into()),
                    )),
                }
            }
            _ => Ok(self.line_filter()?.map(Stage::Line)),
        }
    }

    fn line_filter(&mut self) -> Result<Option<LineFilter>, ParseError> {
        let op = match self.scanner.peek_token()? {
            Token::PipeExact => LineFilterOp::Contains,
            Token::Neq => LineFilterOp::NotContains,
            Token::PipeMatch => LineFilterOp::Re,
            Token::Nre => LineFilterOp::Nre,
            _ => return Ok(None),
        };
        self.scanner.next_token()?; // committed now
        let value = match self.scanner.next_token()? {
            Token::String(v) => v,
            other => {
                return Err(ParseError::UnexpectedToken(
                    other,
                    Token::String("string".into()),
                ));
            }
        };
        Ok(Some(LineFilter { op, value }))
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
            (
                "{}",
                Expr::Log(LogQuery {
                    selector: Selector { matchers: vec![] },
                    pipeline: vec![],
                }),
            ),
            (
                r#"{foo="bar"}"#,
                Expr::Log(LogQuery {
                    selector: Selector {
                        matchers: vec![Matcher {
                            name: "foo".into(),
                            op: MatchOp::Eq,
                            value: "bar".into(),
                        }],
                    },
                    pipeline: vec![],
                }),
            ),
            (
                r#"{foo="bar", bar!="baz"}"#,
                Expr::Log(LogQuery {
                    selector: Selector {
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
                    },
                    pipeline: vec![],
                }),
            ),
            (
                r#"{foo=~"bar", bar!~"baz"}"#,
                Expr::Log(LogQuery {
                    selector: Selector {
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
                    },
                    pipeline: vec![],
                }),
            ),
            (
                r#"{foo="bar", bar!="baz"} != "bip""#,
                Expr::Log(LogQuery {
                    selector: Selector {
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
                    },
                    pipeline: vec![Stage::Line(LineFilter {
                        op: LineFilterOp::NotContains,
                        value: "bip".into(),
                    })],
                }),
            ),
            (
                r#"{foo="bar", bar!="baz"} != "bip" !~ ".+bop""#,
                Expr::Log(LogQuery {
                    selector: Selector {
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
                    },
                    pipeline: vec![
                        Stage::Line(LineFilter {
                            op: LineFilterOp::NotContains,
                            value: "bip".into(),
                        }),
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Nre,
                            value: ".+bop".into(),
                        }),
                    ],
                }),
            ),
            (
                r#"{foo="bar"} |= "baz" |~ "blip" != "flip" !~ "flap""#,
                Expr::Log(LogQuery {
                    selector: Selector {
                        matchers: vec![Matcher {
                            name: "foo".into(),
                            op: MatchOp::Eq,
                            value: "bar".into(),
                        }],
                    },
                    pipeline: vec![
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Contains,
                            value: "baz".into(),
                        }),
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Re,
                            value: "blip".into(),
                        }),
                        Stage::Line(LineFilter {
                            op: LineFilterOp::NotContains,
                            value: "flip".into(),
                        }),
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Nre,
                            value: "flap".into(),
                        }),
                    ],
                }),
            ),
            (
                r#"{foo="bar"} |= "baz" | logfmt !~ "flap""#,
                Expr::Log(LogQuery {
                    selector: Selector {
                        matchers: vec![Matcher {
                            name: "foo".into(),
                            op: MatchOp::Eq,
                            value: "bar".into(),
                        }],
                    },
                    pipeline: vec![
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Contains,
                            value: "baz".into(),
                        }),
                        Stage::Logfmt,
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Nre,
                            value: "flap".into(),
                        }),
                    ],
                }),
            ),
            /* |> !>
            (
                r#"{foo="bar", bar!="baz"} |> "<_>" !> "<_> <_>""#,
                Expr::Log(LogQuery {
                    selector: Selector {
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
                    },
                    pipeline: vec![
                        Stage::Line(LineFilter {
                            op: LineFilterOp::NotContains,
                            value: "bip".into(),
                        }),
                        Stage::Line(LineFilter {
                            op: LineFilterOp::Nre,
                            value: ".+bop".into(),
                        }),
                    ],
                }),
            ),
            */
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
