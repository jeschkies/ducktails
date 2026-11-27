//! Integration tests: these see only the public API, so they fail to compile if
//! something the library is supposed to expose is not actually reachable.

use ducktails::parser::{Expr, MatchOp, Parser};

#[test]
fn parses_a_selector_through_the_public_api() {
    let Expr::Log(query) = Parser::parse(r#"{filename=~".*app.*"}"#).expect("should parse");
    let selector = query.selector;

    assert_eq!(selector.matchers.len(), 1);
    assert_eq!(selector.matchers[0].name, "filename");
    assert_eq!(selector.matchers[0].op, MatchOp::Re);
    assert_eq!(selector.matchers[0].value, ".*app.*");
}

#[test]
fn reports_a_parse_error_as_a_readable_message() {
    // The CLI hands this string to clap, so its wording is user-facing.
    let err = Parser::parse("{foo=}").expect_err("should not parse");

    assert_eq!(
        err.to_string(),
        r#"Unexpected token: '}', expected: "string""#
    );
}
