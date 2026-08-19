use crate::scanner::Token;
use std::{error::Error, fmt};

#[derive(Debug)]
pub enum ParseError {
    UnexpectedChar(char),
    UnexpectedToken(Token, Token),
    UnexpectedEOL,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedChar(t) => write!(f, "Unexpected char: {t}"),
            ParseError::UnexpectedToken(actual, expected) => {
                write!(f, "Unexpected token: {actual}, expected: {expected}")
            }
            ParseError::UnexpectedEOL => write!(f, "Unexpected end of line"),
        }
    }
}

impl Error for ParseError {}
