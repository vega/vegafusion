use pyo3::prelude::*;


#[pyclass]
struct PySpecPlan {
    #[pyo3(get)]
    full_spec: String,

    #[pyo3(get)]
    server_spec: String,

    #[pyo3(get)]
    client_spec: String,
}

#[pymethods]
impl PySpecPlan {
    #[new]
    fn new(full_spec: String, server_spec: String, client_spec: String) -> Self {
        Self {
            full_spec, server_spec, client_spec
        }
    }
}



/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn vegafusion(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySpecPlan>()?;
    Ok(())
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
