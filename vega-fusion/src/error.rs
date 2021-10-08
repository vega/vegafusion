use std::result;
use thiserror::Error;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use std::num::ParseFloatError;

pub type Result<T> = result::Result<T, VegaFusionError>;

#[derive(Clone, Debug, Default)]
pub struct ErrorContext {
    pub contexts: Vec<String>,
}

impl std::fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (i, context) in self.contexts.iter().enumerate() {
            writeln!(f, "    Context[{}]: {}", i, context)?;
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum VegaFusionError {
    #[error("Expression parsing error: {0}\n{1}")]
    ParseError(String, ErrorContext),

    #[error("Expression compilation error: {0}\n{1}")]
    CompilationError(String, ErrorContext),

    #[error("Internal error: {0}\n{1}")]
    InternalError(String, ErrorContext),

    #[error("Vega Specification error: {0}\n{1}")]
    SpecificationError(String, ErrorContext),

    #[error("DataFusion error: {0}\n{1}")]
    DataFusionError(DataFusionError, ErrorContext),

    #[error("IO Error: {0}\n{1}")]
    IOError(std::io::Error, ErrorContext),

    #[error("IO Error: {0}\n{1}")]
    SerdeJsonError(serde_json::Error, ErrorContext),
}

impl VegaFusionError {
    /// Append a new context level to the error
    pub fn with_context<S, F>(self, context_fn: F) -> Self
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        use VegaFusionError::*;
        match self {
            ParseError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::ParseError(msg, context)
            }
            CompilationError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::CompilationError(msg, context)
            }
            InternalError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::CompilationError(msg, context)
            }
            SpecificationError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::SpecificationError(msg, context)
            }
            DataFusionError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::DataFusionError(err, context)
            }
            IOError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::IOError(err, context)
            }
            SerdeJsonError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::SerdeJsonError(err, context)
            }
        }
    }

    pub fn parse(message: &str) -> Self {
        Self::ParseError(message.to_string(), Default::default())
    }

    pub fn compilation(message: &str) -> Self {
        Self::CompilationError(message.to_string(), Default::default())
    }

    pub fn internal(message: &str) -> Self {
        Self::InternalError(message.to_string(), Default::default())
    }

    pub fn specification(message: &str) -> Self {
        Self::SpecificationError(message.to_string(), Default::default())
    }
}

pub trait ResultWithContext<R> {
    fn with_context<S, F>(self, context_fn: F) -> Result<R>
    where
        F: FnOnce() -> S,
        S: Into<String>;
}

impl<R, E> ResultWithContext<R> for result::Result<R, E>
where
    E: Into<VegaFusionError>,
{
    fn with_context<S, F>(self, context_fn: F) -> Result<R>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        match self {
            Ok(val) => Ok(val),
            Err(err) => {
                let vega_fusion_error: VegaFusionError = err.into();
                Err(vega_fusion_error.with_context(context_fn))
            }
        }
    }
}

impl<R> ResultWithContext<R> for Option<R> {
    fn with_context<S, F>(self, context_fn: F) -> Result<R>
    where
        F: FnOnce() -> S,
        S: Into<String>,
    {
        match self {
            Some(val) => Ok(val),
            None => Err(VegaFusionError::internal(&context_fn().into())),
        }
    }
}

impl From<ParseFloatError> for VegaFusionError {
    fn from(err: ParseFloatError) -> Self {
        Self::parse(&err.to_string())
    }
}

impl From<DataFusionError> for VegaFusionError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err, Default::default())
    }
}

impl From<ArrowError> for VegaFusionError {
    fn from(err: ArrowError) -> Self {
        Self::DataFusionError(DataFusionError::ArrowError(err), Default::default())
    }
}

impl From<std::io::Error> for VegaFusionError {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(err, Default::default())
    }
}

impl From<serde_json::Error> for VegaFusionError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerdeJsonError(err, Default::default())
    }
}

pub trait ToInternalError<T> {
    fn internal(self, context: &str) -> Result<T>;
}

impl<T, E: std::error::Error> ToInternalError<T> for std::result::Result<T, E> {
    fn internal(self, context: &str) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => {
                let context = ErrorContext {
                    contexts: vec![context.to_string()],
                };
                Err(VegaFusionError::InternalError(err.to_string(), context))
            }
        }
    }
}
