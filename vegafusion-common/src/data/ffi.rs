use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

/// Validate PyCapsule has provided name
fn validate_pycapsule_name(capsule: &PyCapsule, expected_name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if let Some(capsule_name) = capsule_name {
        let capsule_name = capsule_name.to_str()?;
        if capsule_name != expected_name {
            return Err(PyValueError::new_err(format!(
                "Expected name '{}' in PyCapsule, instead got '{}'",
                expected_name, capsule_name
            )));
        }
    } else {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    Ok(())
}

/// Import `__arrow_c_stream__` across Python boundary.
fn call_arrow_c_stream(ob: &'_ PyAny) -> PyResult<&'_ PyCapsule> {
    if !ob.hasattr("__arrow_c_stream__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_stream__",
        ));
    }

    let capsule = ob.getattr("__arrow_c_stream__")?.call0()?.downcast()?;
    Ok(capsule)
}

fn import_stream_pycapsule(capsule: &PyCapsule) -> PyResult<FFI_ArrowArrayStream> {
    validate_pycapsule_name(capsule, "arrow_array_stream")?;

    let stream = unsafe { FFI_ArrowArrayStream::from_raw(capsule.pointer() as _) };
    Ok(stream)
}

pub(crate) fn import_arrow_c_stream(ob: &'_ PyAny) -> PyResult<(Vec<RecordBatch>, SchemaRef)> {
    let capsule = call_arrow_c_stream(ob)?;
    let stream = import_stream_pycapsule(capsule)?;
    let stream_reader = ArrowArrayStreamReader::try_new(stream)
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let schema = stream_reader.schema();

    let mut batches = vec![];
    for batch in stream_reader {
        let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
        batches.push(batch);
    }

    Ok((batches, schema))
}
