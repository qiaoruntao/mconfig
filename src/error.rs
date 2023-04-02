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

// impl de::Error for ConfigError {
//     fn custom<T: fmt::Display>(msg: T) -> Self {
//         Self::Message(msg.to_string())
//     }
// }
//
// impl ser::Error for ConfigError {
//     fn custom<T: Display>(msg: T) -> Self {
//         Self::Message(msg.to_string())
//     }
// }