use std::result;
use thiserror::Error;

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

    #[error("DataFusion error: {0}\n{1}")]
    DataFusionError(DataFusionError, ErrorContext),
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
            DataFusionError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::DataFusionError(err, context)
            }
        }
    }

    pub fn parse_error(message: &str) -> Self {
        Self::ParseError(message.to_string(), Default::default())
    }

    pub fn compilation_error(message: &str) -> Self {
        Self::CompilationError(message.to_string(), Default::default())
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

impl From<ParseFloatError> for VegaFusionError {
    fn from(err: ParseFloatError) -> Self {
        Self::parse_error(&err.to_string())
    }
}

impl From<DataFusionError> for VegaFusionError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err, Default::default())
    }
}
