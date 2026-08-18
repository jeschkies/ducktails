use std::{error::Error, fmt};

#[derive(Debug)]
pub enum ParseError {
    UnexpectedToken(char),
    UnexpectedEOL,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            &ParseError::UnexpectedToken(t) => write!(f, "Unexpected Token: {t}"),
            &ParseError::UnexpectedEOL => write!(f, "Unexpected end of line"),
        }
    }
}

impl Error for ParseError {}
