use std::{fmt, result};
use std::fmt::{Debug, Display, Formatter};

pub enum MConfigError {
    MongodbError(mongodb::error::Error),
    KeyNotExists {
        key: String
    },
}

pub type Result<T> = result::Result<T, MConfigError>;

impl Debug for MConfigError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", *self)
    }
}

impl Display for MConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            MConfigError::MongodbError(e) => {
                write!(f, "mongodb error {}", e)
            }
            MConfigError::KeyNotExists { key } => {
                write!(f, "key {} not found", key)
            }
        }
    }
}

impl std::error::Error for MConfigError {}