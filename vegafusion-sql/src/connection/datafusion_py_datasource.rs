use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use datafusion_common::{project_schema, DataFusionError, Statistics};
use datafusion_expr::{Expr, TableType};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyTuple;
use pyo3::{Bound, IntoPy, PyAny, PyErr, PyObject, Python, ToPyObject};
use pyo3_arrow::PySchema;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;

#[derive(Debug, Clone)]
pub struct PyDatasource {
    py_datasource: Arc<PyObject>,
    schema: SchemaRef,
}

impl PyDatasource {
    pub fn try_new(py_datasource: &Bound<PyAny>) -> Result<Self, PyErr> {
        Python::with_gil(|py| -> Result<_, PyErr> {
            let table_schema_obj = py_datasource.call_method0("schema")?;
            let schema = table_schema_obj.extract::<PySchema>()?;

            Ok(Self {
                py_datasource: Arc::new(py_datasource.to_object(py)),
                schema: schema.into_inner(),
            })
        })
    }

    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PyDatasourceExec::new(
            projections,
            schema,
            self.clone(),
        )))
    }
}

#[async_trait]
impl TableProvider for PyDatasource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &(dyn datafusion::catalog::Session),
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, self.schema()).await;
    }
}

#[derive(Debug, Clone)]
struct PyDatasourceExec {
    db: PyDatasource,
    projected_schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl PyDatasourceExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: PyDatasource) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let plan_properties = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            plan_properties,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for PyDatasourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PyDatasourceExec")
    }
}

impl ExecutionPlan for PyDatasourceExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let table = Python::with_gil(|py| -> Result<_, PyErr> {
            let column_names = self
                .projected_schema
                .fields
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>();
            let args = PyTuple::new_bound(py, vec![column_names.into_py(py)]);
            let pa_table = self.db.py_datasource.call_method1(py, "fetch", args)?;
            let table = VegaFusionTable::from_pyarrow(py, pa_table.bind(py))?;
            Ok(table)
        })
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;

        Ok(Box::pin(MemoryStream::try_new(
            table.batches,
            table.schema,
            None,
        )?))
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn name(&self) -> &str {
        "py_datasource"
    }
}
