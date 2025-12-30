mod node;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Uninterpretable,
    KeyExists,
    KeyNotFound,
    Overflow,
}
