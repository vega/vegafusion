use arrow::datatypes::{Schema, SchemaRef};
use arrow::pyarrow::FromPyArrow;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_common::{project_schema, DataFusionError, Statistics};
use datafusion_expr::{Expr, TableType};
use pyo3::types::PyTuple;
use pyo3::{IntoPy, PyErr, PyObject, Python};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use vegafusion_common::data::table::VegaFusionTable;

#[derive(Debug, Clone)]
pub struct PyDatasource {
    py_datasource: PyObject,
    schema: SchemaRef,
}

impl PyDatasource {
    pub fn try_new(py_datasource: PyObject) -> Result<Self, PyErr> {
        Python::with_gil(|py| -> Result<_, PyErr> {
            let table_schema_obj = py_datasource.call_method0(py, "schema")?;
            let schema = Arc::new(Schema::from_pyarrow(table_schema_obj.as_ref(py))?);
            Ok(Self {
                py_datasource,
                schema,
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
        _state: &SessionState,
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
}

impl PyDatasourceExec {
    fn new(projections: Option<&Vec<usize>>, schema: SchemaRef, db: PyDatasource) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        Self {
            db,
            projected_schema,
        }
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

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
            let args = PyTuple::new(py, vec![column_names.into_py(py)]);
            let pa_table = self.db.py_datasource.call_method1(py, "fetch", args)?;
            let table = VegaFusionTable::from_pyarrow(py, &pa_table.as_ref(py))?;
            Ok(table)
        })
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;

        Ok(Box::pin(MemoryStream::try_new(
            table.batches,
            table.schema,
            None,
        )?))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
