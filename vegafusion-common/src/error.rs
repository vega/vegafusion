use arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use std::num::ParseFloatError;
use std::result;
use thiserror::Error;

#[cfg(feature = "datafusion-proto")]
use datafusion_proto::logical_plan::to_proto::Error as DataFusionProtoError;

#[cfg(feature = "pyo3")]
use pyo3::{exceptions::PyValueError, PyErr};

#[cfg(feature = "jni")]
use jni::errors::Error as JniError;

#[cfg(feature = "base64")]
use base64::DecodeError as Base64DecodeError;

#[cfg(feature = "object_store")]
use object_store::{path::Error as ObjectStorePathError, Error as ObjectStoreError};

pub type Result<T> = result::Result<T, VegaFusionError>;

#[derive(Clone, Debug, Default)]
pub struct ErrorContext {
    pub contexts: Vec<String>,
}

impl std::fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (i, context) in self.contexts.iter().enumerate() {
            writeln!(f, "    Context[{i}]: {context}")?;
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

    #[error("SQL Not Supported error: {0}\n{1}")]
    SqlNotSupported(String, ErrorContext),

    #[error("Arrow error: {0}\n{1}")]
    FormatError(std::fmt::Error, ErrorContext),

    #[error("Arrow error: {0}\n{1}")]
    ArrowError(ArrowError, ErrorContext),

    #[error("DataFusion error: {0}\n{1}")]
    DataFusionError(DataFusionError, ErrorContext),

    #[cfg(feature = "datafusion-proto")]
    #[error("DataFusion proto error: {0}\n{1}")]
    DataFusionProtoError(DataFusionProtoError, ErrorContext),

    #[error("IO Error: {0}\n{1}")]
    IOError(std::io::Error, ErrorContext),

    #[cfg(feature = "pyo3")]
    #[error("Python Error: {0}\n{1}")]
    PythonError(pyo3::PyErr, ErrorContext),

    #[cfg(feature = "json")]
    #[error("Serde JSON Error: {0}\n{1}")]
    SerdeJsonError(serde_json::Error, ErrorContext),

    #[cfg(feature = "sqlparser")]
    #[error("SqlParser Error: {0}\n{1}")]
    SqlParserError(sqlparser::parser::ParserError, ErrorContext),

    #[cfg(feature = "jni")]
    #[error("JNI Error: {0}\n{1}")]
    JniError(JniError, ErrorContext),

    #[cfg(feature = "base64")]
    #[error("Base64 Decode Error: {0}\n{1}")]
    Base64DecodeError(Base64DecodeError, ErrorContext),

    #[cfg(feature = "object_store")]
    #[error("ObjectStoreError Error: {0}\n{1}")]
    ObjectStoreError(ObjectStoreError, ErrorContext),
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
            SqlNotSupported(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::SqlNotSupported(msg, context)
            }
            FormatError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::FormatError(msg, context)
            }
            ArrowError(msg, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::ArrowError(msg, context)
            }
            DataFusionError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::DataFusionError(err, context)
            }
            #[cfg(feature = "datafusion-proto")]
            DataFusionProtoError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::DataFusionProtoError(err, context)
            }
            IOError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::IOError(err, context)
            }
            #[cfg(feature = "pyo3")]
            PythonError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::PythonError(err, context)
            }
            #[cfg(feature = "json")]
            SerdeJsonError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::SerdeJsonError(err, context)
            }
            #[cfg(feature = "sqlparser")]
            SqlParserError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::SqlParserError(err, context)
            }
            #[cfg(feature = "jni")]
            JniError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::JniError(err, context)
            }
            #[cfg(feature = "base64")]
            Base64DecodeError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::Base64DecodeError(err, context)
            }
            #[cfg(feature = "object_store")]
            ObjectStoreError(err, mut context) => {
                context.contexts.push(context_fn().into());
                VegaFusionError::ObjectStoreError(err, context)
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

    pub fn sql_not_supported<S: Into<String>>(message: S) -> Self {
        Self::SqlNotSupported(message.into(), Default::default())
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
            SqlNotSupported(msg, context) => {
                VegaFusionError::SqlNotSupported(msg.clone(), context.clone())
            }
            FormatError(err, context) => VegaFusionError::FormatError(*err, context.clone()),
            ArrowError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            DataFusionError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            #[cfg(feature = "datafusion-proto")]
            DataFusionProtoError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            IOError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            #[cfg(feature = "pyo3")]
            PythonError(msg, context) => {
                VegaFusionError::ExternalError(msg.to_string(), context.clone())
            }
            #[cfg(feature = "json")]
            SerdeJsonError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            #[cfg(feature = "sqlparser")]
            SqlParserError(err, context) => {
                VegaFusionError::SqlParserError(err.clone(), context.clone())
            }
            #[cfg(feature = "jni")]
            JniError(err, context) => {
                VegaFusionError::ExternalError(err.to_string(), context.clone())
            }
            #[cfg(feature = "base64")]
            Base64DecodeError(err, context) => {
                VegaFusionError::Base64DecodeError(err.clone(), context.clone())
            }
            #[cfg(feature = "object_store")]
            ObjectStoreError(err, context) => {
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
            None => Err(VegaFusionError::internal(context_fn().into())),
        }
    }
}

impl From<ParseFloatError> for VegaFusionError {
    fn from(err: ParseFloatError) -> Self {
        Self::parse(err.to_string())
    }
}

impl From<DataFusionError> for VegaFusionError {
    fn from(err: DataFusionError) -> Self {
        Self::DataFusionError(err, Default::default())
    }
}

#[cfg(feature = "datafusion-proto")]
impl From<DataFusionProtoError> for VegaFusionError {
    fn from(err: DataFusionProtoError) -> Self {
        Self::DataFusionProtoError(err, Default::default())
    }
}

impl From<std::fmt::Error> for VegaFusionError {
    fn from(err: std::fmt::Error) -> Self {
        Self::FormatError(err, Default::default())
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

#[cfg(feature = "pyo3")]
impl From<PyErr> for VegaFusionError {
    fn from(err: PyErr) -> Self {
        Self::PythonError(err, Default::default())
    }
}

#[cfg(feature = "json")]
impl From<serde_json::Error> for VegaFusionError {
    fn from(err: serde_json::Error) -> Self {
        Self::SerdeJsonError(err, Default::default())
    }
}

#[cfg(feature = "sqlparser")]
impl From<sqlparser::parser::ParserError> for VegaFusionError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        Self::SqlParserError(err, Default::default())
    }
}

#[cfg(feature = "jni")]
impl From<JniError> for VegaFusionError {
    fn from(err: JniError) -> Self {
        Self::JniError(err, Default::default())
    }
}

#[cfg(feature = "base64")]
impl From<Base64DecodeError> for VegaFusionError {
    fn from(err: Base64DecodeError) -> Self {
        Self::Base64DecodeError(err, Default::default())
    }
}

#[cfg(feature = "object_store")]
impl From<ObjectStoreError> for VegaFusionError {
    fn from(err: ObjectStoreError) -> Self {
        Self::ObjectStoreError(err, Default::default())
    }
}

#[cfg(feature = "object_store")]
impl From<ObjectStorePathError> for VegaFusionError {
    fn from(err: ObjectStorePathError) -> Self {
        Self::ObjectStoreError(
            ObjectStoreError::InvalidPath { source: err },
            Default::default(),
        )
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
