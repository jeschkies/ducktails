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
        // TODO(human): drive the scanner to build a Selector.
        //
        // Shape, from `syntax.y`:
        //     selector: '{' matchers '}' | '{' '}'
        //     matchers: matcher | matchers ',' matcher
        //     matcher:  IDENTIFIER (= | != | =~ | !~) STRING
        //
        // Two decisions to make, and they are the interesting part:
        //
        //  1. `Scanner` yields `Token`, which has no `Identifier` or `String`
        //     variant yet. Do you extend `Token` with payload-carrying variants
        //     (`Identifier(String)`, `Str(String)`), or have the parser call
        //     `scanner.advance()` directly to read them? The first keeps all
        //     lexing in the scanner; the second avoids owning Strings per token.
        //
        //  2. `ParseError` currently only speaks in `char`s
        //     (`UnexpectedToken(char)`). A parser needs to say "expected `}`,
        //     found `,`" — so it likely wants a `Token`-level variant. What
        //     should that look like?
        let _ = scanner;
        todo!("parse a stream selector")
    }
}
