use crate::scanner::Token;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum ParseError {
    UnexpectedChar(char),
    UnexpectedToken(Token, Token),
    UnexpectedEOL,
    UnknownFunction(String),
    UnknownParser(String),
    Unsupported,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedChar(t) => write!(f, "Unexpected char: {t}"),
            ParseError::UnexpectedToken(actual, expected) => {
                write!(f, "Unexpected token: '{actual}', expected: {expected}")
            }
            ParseError::UnexpectedEOL => write!(f, "Unexpected end of line"),
            ParseError::UnknownFunction(name) => write!(f, "Unknown function: {name}"),
            ParseError::UnknownParser(name) => write!(f, "Unknown pipeline stage: {name}"),
            ParseError::Unsupported => write!(f, "Unsupported"),
        }
    }
}

impl Error for ParseError {}
