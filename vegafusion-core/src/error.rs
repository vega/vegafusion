/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::proto::gen::errors::Error as ProtoError;
use arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use std::num::ParseFloatError;
use std::result;
use thiserror::Error;

use crate::proto::gen::errors::error::Errorkind;
#[cfg(feature = "pyo3")]
use pyo3::{exceptions::PyValueError, PyErr};

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

    #[error("External error: {0}\n{1}")]
    ExternalError(String, ErrorContext),

    #[error("Vega Specification error: {0}\n{1}")]
    SpecificationError(String, ErrorContext),

    #[error("Pre-transform error: {0}\n{1}")]
    PreTransformError(String, ErrorContext),

    #[error("Arrow error: {0}\n{1}")]
    ArrowError(ArrowError, ErrorContext),

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
                VegaFusionError::InternalError(msg, context)
            }
            ExternalError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::ExternalError(msg, context)
            }
            SpecificationError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::SpecificationError(msg, context)
            }
            PreTransformError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::PreTransformError(msg, context)
            }
            ArrowError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::ArrowError(msg, context)
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

    pub fn parse<S: Into<String>>(message: S) -> Self {
        Self::ParseError(message.into(), Default::default())
    }

    pub fn compilation<S: Into<String>>(message: S) -> Self {
        Self::CompilationError(message.into(), Default::default())
    }

    pub fn internal<S: Into<String>>(message: S) -> Self {
        Self::InternalError(message.into(), Default::default())
    }

    pub fn external<S: Into<String>>(message: S) -> Self {
        Self::ExternalError(message.into(), Default::default())
    }

    pub fn specification<S: Into<String>>(message: S) -> Self {
        Self::SpecificationError(message.into(), Default::default())
    }

    pub fn pre_transform<S: Into<String>>(message: S) -> Self {
        Self::PreTransformError(message.into(), Default::default())
    }

    /// Duplicate error. Not a precise Clone because some of the wrapped error types aren't Clone
    /// These are converted to internal errors
    pub fn duplicate(&self) -> Self {
        use VegaFusionError::*;
        match self {
            ParseError(msg, context) => VegaFusionError::ParseError(msg.clone(), context.clone()),
            CompilationError(msg, context) => {
                VegaFusionError::CompilationError(msg.clone(), context.clone())
            }
            InternalError(msg, context) => {
                VegaFusionError::InternalError(msg.clone(), context.clone())
            }
            ExternalError(msg, context) => {
                VegaFusionError::ExternalError(msg.clone(), context.clone())
            }
            SpecificationError(msg, context) => {
                VegaFusionError::SpecificationError(msg.clone(), context.clone())
            }
            PreTransformError(msg, context) => {
                VegaFusionError::PreTransformError(msg.clone(), context.clone())
            }
            ArrowError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            DataFusionError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            IOError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            SerdeJsonError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
        }
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
        Self::ArrowError(err, Default::default())
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

pub trait ToExternalError<T> {
    fn external<S: Into<String>>(self, context: S) -> Result<T>;
}

impl<T, E: std::error::Error> ToExternalError<T> for std::result::Result<T, E> {
    fn external<S: Into<String>>(self, context: S) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(err) => {
                let context = ErrorContext {
                    contexts: vec![context.into()],
                };
                Err(VegaFusionError::ExternalError(err.to_string(), context))
            }
        }
    }
}

pub trait DuplicateResult {
    fn duplicate(&self) -> Self;
}

impl<T> DuplicateResult for Result<T>
where
    T: Clone,
{
    fn duplicate(&self) -> Self {
        match self {
            Ok(v) => Ok(v.clone()),
            Err(err) => Err(err.duplicate()),
        }
    }
}

// Conversion to PyO3 error
#[cfg(feature = "pyo3")]
impl std::convert::From<VegaFusionError> for PyErr {
    fn from(err: VegaFusionError) -> PyErr {
        PyValueError::new_err(err.to_string())
    }
}

impl ProtoError {
    pub fn msg(&self) -> String {
        match self.errorkind.as_ref().unwrap() {
            Errorkind::Error(e) => e.msg.clone(),
        }
    }
}
