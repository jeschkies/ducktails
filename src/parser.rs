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

pub struct Parser;

impl Parser {
    pub fn parse(query: &str) -> Result<Expr, ParseError> {
        let mut scanner = Scanner::new(query);
        let selector = Self::selector(&mut scanner)?;
        Ok(Expr::Log(selector))
    }

    /// Parse `{ name op "value", ... }`.
    fn selector(scanner: &mut Scanner<'_>) -> Result<Selector, ParseError> {
        let t = scanner.next_token()?;
        if t != Token::OpenBrace {
            return Err(ParseError::UnexpectedToken(t, Token::OpenBrace));
        }

        let mut matchers = Vec::new();
        while scanner.peek_token()? != Token::CloseBrace {
            let m = Parser::matcher(scanner)?;
            matchers.push(m);
        }
        // consume CloseBrace
        scanner.next_token()?;

        Ok(Selector { matchers })
    }

    fn matcher(scanner: &mut Scanner<'_>) -> Result<Matcher, ParseError> {
        if let Token::Identifier(_identifier) = scanner.next_token()? {
            todo!("expect operator and value")
        } else {
            return Err(ParseError::UnexpectedEOL); // TODO: create custom error
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_selector() {
        let cases = [
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
        ];
        for (input, want) in cases {
            let got = Parser::parse(input).unwrap_or_else(|e| panic!("input {input:?}: {e}"));
            assert_eq!(got, want, "input: {input:?}");
        }
    }
}
