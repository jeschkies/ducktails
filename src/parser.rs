use std::time::Duration;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelFilterOp {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Neq,
    Nre,
    Re,
}

/// The right-hand side of a label filter, as the scanner typed it.
///
/// No `Eq`, here or in anything holding one: `Number` is an `f64`. The scanner
/// has already resolved units, so `10GB` arrives as `Bytes(10_000_000_000)`.
#[derive(Debug, Clone, PartialEq)]
pub enum LabelValue {
    String(String), // matcher            = != =~ !~
    Number(f64),    // numberFilter       = != == > >= < <=
    Duration(Duration),
    Bytes(u64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LabelFilter {
    pub label: String,
    pub op: LabelFilterOp,
    pub value: LabelValue,
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
#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    LabelFilter(LabelFilter),
    Line(LineFilter),
    Logfmt,
} // Json / LabelFilter later

#[derive(Debug, Clone, PartialEq)]
pub struct LogQuery {
    pub selector: Selector,
    pub pipeline: Vec<Stage>,
}

/// A log range aggregation: a function over the lines in a time window.
///
/// Only the four that count lines or bytes; `absent_over_time` needs "no rows
/// yields 1", which an aggregate cannot produce without a grid of instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOp {
    CountOverTime,
    Rate,
    BytesOverTime,
    BytesRate,
}

impl RangeOp {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "count_over_time" => RangeOp::CountOverTime,
            "rate" => RangeOp::Rate,
            "bytes_over_time" => RangeOp::BytesOverTime,
            "bytes_rate" => RangeOp::BytesRate,
            _ => return None,
        })
    }
}

/// `count_over_time({app="x"} |= "error" [5m])`.
///
/// The range selector binds to the log query inside the parentheses, so the
/// pipeline is parsed first and `[5m]` closes it.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeAggregation {
    pub op: RangeOp,
    pub range: Duration,
    pub query: LogQuery,
}

/// The root of a LogQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A log query: a selector plus a pipeline.
    Log(LogQuery),
    /// A metric query. Only range aggregations so far; `sum`, `avg` and the
    /// other vector operators wrap one of these.
    Metric(RangeAggregation),
}

/// The `expected` side of a parse error names a *kind* of token, and `Duration`
/// has no spelling for "any duration", so an error reports `[0s]`.
fn d_zero() -> Duration {
    Duration::ZERO
}

pub struct Parser<'a> {
    scanner: Scanner<'a>,
}

impl<'a> Parser<'a> {
    pub fn parse(query: &str) -> Result<Expr, ParseError> {
        let mut p = Parser {
            scanner: Scanner::new(query),
        };
        let expr = p.expr()?;
        p.expect(Token::Eol)?;
        Ok(expr)
    }

    /// A query is either a log query, which starts with a selector, or a metric
    /// query, which starts with the name of a function.
    fn expr(&mut self) -> Result<Expr, ParseError> {
        match self.scanner.peek_token()? {
            Token::Identifier(name) => {
                self.scanner.next_token()?; // the name is ours now
                Ok(Expr::Metric(self.range_aggregation(name)?))
            }
            _ => Ok(Expr::Log(self.log_query()?)),
        }
    }

    /// Parse `count_over_time({app="x"} |= "error" [5m])`, the name already read.
    fn range_aggregation(&mut self, name: String) -> Result<RangeAggregation, ParseError> {
        let op = RangeOp::from_name(&name).ok_or(ParseError::UnknownFunction(name))?;

        self.expect(Token::OpenParenthesis)?;
        let query = self.log_query()?;

        self.expect(Token::OpenBracket)?;
        let range = match self.scanner.next_token()? {
            Token::Duration(d) => d,
            other => {
                return Err(ParseError::UnexpectedToken(
                    other,
                    Token::Duration(d_zero()),
                ));
            }
        };
        self.expect(Token::CloseBracket)?;
        self.expect(Token::CloseParenthesis)?;

        Ok(RangeAggregation { op, range, query })
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
                    Token::Identifier(label) => match self.label_filter(label.clone())? {
                        Some(filter) => Ok(Some(Stage::LabelFilter(filter))),
                        // A pipe commits to a stage, so an identifier with no
                        // operator after it is a parser we do not have — `| json`,
                        // say — and not the end of the pipeline. Returning `None`
                        // here would drop the stage and answer the wrong query.
                        None => Err(ParseError::UnknownParser(label)),
                    },
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

    fn label_filter(&mut self, label: String) -> Result<Option<LabelFilter>, ParseError> {
        let op = match self.scanner.peek_token()? {
            Token::Eq => LabelFilterOp::Eq,
            Token::Gt => LabelFilterOp::Gt,
            Token::Gte => LabelFilterOp::Gte,
            Token::Lt => LabelFilterOp::Lt,
            Token::Lte => LabelFilterOp::Lte,
            Token::Neq => LabelFilterOp::Neq,
            Token::Nre => LabelFilterOp::Nre,
            Token::Re => LabelFilterOp::Re,
            _ => return Ok(None),
        };
        self.scanner.next_token()?; // committed now
        let value = match self.scanner.next_token()? {
            Token::String(v) => LabelValue::String(v),
            Token::Number(v) => LabelValue::Number(v),
            Token::Duration(v) => LabelValue::Duration(v),
            Token::Bytes(v) => LabelValue::Bytes(v),
            other => {
                return Err(ParseError::UnexpectedToken(
                    other,
                    Token::String("value".into()),
                ));
            }
        };
        Ok(Some(LabelFilter { label, op, value }))
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
            (
                r#"{foo="bar"} |= "baz" | logfmt | level="error""#,
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
                        Stage::LabelFilter(LabelFilter {
                            label: "level".into(),
                            op: LabelFilterOp::Eq,
                            value: LabelValue::String("error".into()),
                        }),
                    ],
                }),
            ),
            (
                r#"{foo="bar"} |= "baz" | logfmt | b>=10GB"#,
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
                        // SI, as Go's `humanize.ParseBytes` reads it: 10 * 1000^3.
                        Stage::LabelFilter(LabelFilter {
                            label: "b".into(),
                            op: LabelFilterOp::Gte,
                            value: LabelValue::Bytes(10_000_000_000),
                        }),
                    ],
                }),
            ),
            (
                r#"{foo="bar"} | logfmt | status>=500"#,
                Expr::Log(LogQuery {
                    selector: Selector {
                        matchers: vec![Matcher {
                            name: "foo".into(),
                            op: MatchOp::Eq,
                            value: "bar".into(),
                        }],
                    },
                    pipeline: vec![
                        Stage::Logfmt,
                        Stage::LabelFilter(LabelFilter {
                            label: "status".into(),
                            op: LabelFilterOp::Gte,
                            value: LabelValue::Number(500.0),
                        }),
                    ],
                }),
            ),
            (
                r#"{foo="bar"} | logfmt | latency>1h30m"#,
                Expr::Log(LogQuery {
                    selector: Selector {
                        matchers: vec![Matcher {
                            name: "foo".into(),
                            op: MatchOp::Eq,
                            value: "bar".into(),
                        }],
                    },
                    pipeline: vec![
                        Stage::Logfmt,
                        Stage::LabelFilter(LabelFilter {
                            label: "latency".into(),
                            op: LabelFilterOp::Gt,
                            value: LabelValue::Duration(Duration::from_secs(5400)),
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

        let cases: [(&str, Check, &str); 13] = [
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
                // A query starts with either a selector or a function, so a bare
                // identifier here is read as the latter — which also gives the
                // right message for a misspelled `count_over_time`.
                r#"foo="bar""#,
                |e| matches!(e, ParseError::UnknownFunction(n) if n == "foo"),
                "a leading identifier can only be a function name",
            ),
            (
                // The bug this replaced: the pipe was consumed, the stage
                // dropped, and `{} | json` answered as if it were bare `{}`.
                "{} | json",
                |e| matches!(e, ParseError::UnknownParser(n) if n == "json"),
                "an unknown parser stage must not be silently dropped",
            ),
            (
                r#"{} | json |= "error""#,
                |e| matches!(e, ParseError::UnknownParser(n) if n == "json"),
                "and not when something follows it either",
            ),
            (
                // No operator, so it is not a label filter; no parser by that
                // name either.
                "{} | level",
                |e| matches!(e, ParseError::UnknownParser(n) if n == "level"),
                "a label with no operator is not a stage",
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
