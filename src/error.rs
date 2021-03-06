use thiserror::Error;

pub type ErasureCoderResult<T> = Result<T, ErasureCoderError>;

#[derive(Debug, Error)]
pub enum ErasureCoderError {
    #[error("Matrix returned from jerasure was null.")]
    MatrixNull,
    #[error("Bitmatrix returned from jerasure was null.")]
    BitmatrixNull,
    #[error("Schedule returned from jerasure was null.")]
    ScheduleNull,
    #[error("Expected {0} fragments. There were {1} erasures, but received {2} fragments. {0} - {1} != {2}")]
    InvalidNumberOfFragments(usize, usize, usize),
    #[error("Jerasure returned non-zero return code for decode operation")]
    FailedToDecode,
}
