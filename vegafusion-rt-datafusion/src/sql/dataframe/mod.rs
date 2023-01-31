use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::order::ToSqlOrderByExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::connection::{Connection, SqlConnection};
use datafusion::common::{Column, DFSchema};
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::{
    abs, expr, lit, max, min, when, window_function, AggregateFunction, BuiltInWindowFunction,
    BuiltinScalarFunction, Expr, ExprSchemable, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunction,
};
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::ast::{Query, TableAlias};
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlgen::parser::Parser;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;

use crate::expression::compiler::utils::to_numeric;
use crate::expression::escape::flat_col;
use crate::sql::connection::datafusion_conn::make_datafusion_dialect;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Field;
use std::hash::{Hash, Hasher};
use std::ops::{Add, Div, Sub};
use std::sync::Arc;
use vegafusion_core::arrow::compute::concat_batches;
use vegafusion_core::arrow::datatypes::{Schema, SchemaRef};
use vegafusion_core::arrow::record_batch::RecordBatch;
use vegafusion_core::data::scalar::ScalarValue;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::spec::transform::stack::StackOffsetSpec;

#[async_trait]
pub trait DataFrame: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn schema(&self) -> Schema;

    fn schema_df(&self) -> Result<DFSchema> {
        Ok(DFSchema::try_from(self.schema())?)
    }

    fn connection(&self) -> Arc<dyn Connection>;

    fn fingerprint(&self) -> u64;

    async fn collect(&self) -> Result<VegaFusionTable>;

    async fn collect_flat(&self) -> Result<RecordBatch> {
        let mut arrow_schema = Arc::new(self.schema().into()) as SchemaRef;
        let table = self.collect().await?;
        if let Some(batch) = table.batches.get(0) {
            arrow_schema = batch.schema()
        }
        concat_batches(&arrow_schema, table.batches.as_slice())
            .with_context(|| String::from("Failed to concatenate RecordBatches"))
    }

    async fn sort(&self, _expr: Vec<DfExpr>, _limit: Option<i32>) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("sort not supported"))
    }

    async fn select(&self, _expr: Vec<DfExpr>) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("select not supported"))
    }

    async fn aggregate(
        &self,
        _group_expr: Vec<DfExpr>,
        _aggr_expr: Vec<DfExpr>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "aggregate not supported",
        ))
    }

    async fn joinaggregate(
        &self,
        _group_expr: Vec<DfExpr>,
        _aggr_expr: Vec<DfExpr>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported(
            "joinaggregate not supported",
        ))
    }

    async fn filter(&self, _predicate: Expr) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("filter not supported"))
    }

    async fn limit(&self, _limit: i32) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("limit not supported"))
    }

    async fn fold(
        &self,
        _fields: &[String],
        _value_col: &str,
        _key_col: &str,
        _order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("fold not supported"))
    }

    async fn stack(
        &self,
        _field: &str,
        _orderby: Vec<Expr>,
        _groupby: &[String],
        _start_field: &str,
        _stop_field: &str,
        _mode: StackOffsetSpec,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("stack not supported"))
    }

    async fn impute(
        &self,
        _field: &str,
        _value: ScalarValue,
        _key: &str,
        _groupby: &[String],
        _order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        Err(VegaFusionError::sql_not_supported("impute not supported"))
    }
}

#[derive(Clone)]
pub struct SqlDataFrame {
    prefix: String,
    schema: SchemaRef,
    ctes: Vec<Query>,
    conn: Arc<dyn SqlConnection>,
    dialect: Arc<Dialect>,
}

#[async_trait]
impl DataFrame for SqlDataFrame {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> Schema {
        self.schema.as_ref().clone()
    }

    fn connection(&self) -> Arc<dyn Connection> {
        self.conn.to_connection()
    }

    fn fingerprint(&self) -> u64 {
        let mut hasher = deterministic_hash::DeterministicHasher::new(DefaultHasher::new());

        // Add connection id in hash
        self.conn.id().hash(&mut hasher);

        // Add query to hash
        let query_str = self.as_query().sql(self.conn.dialect()).unwrap();
        query_str.hash(&mut hasher);

        hasher.finish()
    }

    async fn collect(&self) -> Result<VegaFusionTable> {
        let query_string = self.as_query().sql(self.conn.dialect())?;
        self.conn.fetch_query(&query_string, &self.schema).await
    }

    async fn sort(&self, expr: Vec<DfExpr>, limit: Option<i32>) -> Result<Arc<dyn DataFrame>> {
        let mut query = self.make_select_star();
        let sql_exprs = expr
            .iter()
            .map(|expr| expr.to_sql_order())
            .collect::<Result<Vec<_>>>()?;
        query.order_by = sql_exprs;
        if let Some(limit) = limit {
            query.limit = Some(lit(limit).to_sql().unwrap())
        }
        self.chain_query(query, self.schema.as_ref().clone()).await
    }

    async fn select(&self, expr: Vec<DfExpr>) -> Result<Arc<dyn DataFrame>> {
        let sql_expr_strs = expr
            .iter()
            .map(|expr| Ok(expr.to_sql_select()?.sql(&self.dialect)?))
            .collect::<Result<Vec<_>>>()?;

        let select_csv = sql_expr_strs.join(", ");
        let query = Parser::parse_sql_query(&format!(
            "select {select_csv} from {parent}",
            select_csv = select_csv,
            parent = self.parent_name()
        ))?;

        // Build new schema
        let new_schema = make_new_schema_from_exprs(self.schema.as_ref(), expr.as_slice())?;

        self.chain_query(query, new_schema).await
    }

    async fn aggregate(
        &self,
        group_expr: Vec<DfExpr>,
        aggr_expr: Vec<DfExpr>,
    ) -> Result<Arc<dyn DataFrame>> {
        // Add group exprs to aggregates for SQL query
        let mut all_aggr_expr = aggr_expr.clone();
        all_aggr_expr.extend(group_expr.clone());

        let sql_group_expr_strs = group_expr
            .iter()
            .map(|expr| Ok(expr.to_sql()?.sql(&self.dialect)?))
            .collect::<Result<Vec<_>>>()?;

        let sql_aggr_expr_strs = all_aggr_expr
            .iter()
            .map(|expr| Ok(expr.to_sql_select()?.sql(&self.dialect)?))
            .collect::<Result<Vec<_>>>()?;

        let aggr_csv = sql_aggr_expr_strs.join(", ");

        let query = if sql_group_expr_strs.is_empty() {
            Parser::parse_sql_query(&format!(
                "select {aggr_csv} from {parent}",
                aggr_csv = aggr_csv,
                parent = self.parent_name(),
            ))?
        } else {
            let group_by_csv = sql_group_expr_strs.join(", ");
            Parser::parse_sql_query(&format!(
                "select {aggr_csv} from {parent} group by {group_by_csv}",
                aggr_csv = aggr_csv,
                parent = self.parent_name(),
                group_by_csv = group_by_csv
            ))?
        };

        // Build new schema from aggregate expressions
        let new_schema =
            make_new_schema_from_exprs(self.schema.as_ref(), all_aggr_expr.as_slice())?;
        self.chain_query(query, new_schema).await
    }

    async fn joinaggregate(
        &self,
        group_expr: Vec<DfExpr>,
        aggr_expr: Vec<DfExpr>,
    ) -> Result<Arc<dyn DataFrame>> {
        let schema = self.schema_df()?;
        let dialect = self.dialect();

        // Build csv str for new columns
        let inner_name = format!("{}_inner", self.parent_name());
        let new_col_names = aggr_expr
            .iter()
            .map(|col| Ok(col.display_name()?))
            .collect::<Result<HashSet<_>>>()?;

        let new_col_strs = aggr_expr
            .iter()
            .map(|col| {
                let col = Expr::Column(Column {
                    relation: Some(inner_name.to_string()),
                    name: col.display_name()?,
                })
                .alias(col.display_name()?);
                Ok(col.to_sql_select()?.sql(dialect)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let new_col_csv = new_col_strs.join(", ");

        // Build csv str of input columns
        let input_col_exprs = schema
            .fields()
            .iter()
            .filter_map(|field| {
                if new_col_names.contains(field.name()) {
                    None
                } else {
                    Some(flat_col(field.name()))
                }
            })
            .collect::<Vec<_>>();

        let input_col_strs = input_col_exprs
            .iter()
            .map(|c| Ok(c.to_sql_select()?.sql(dialect)?))
            .collect::<Result<Vec<_>>>()?;
        let input_col_csv = input_col_strs.join(", ");

        // Perform join aggregation
        let sql_group_expr_strs = group_expr
            .iter()
            .map(|expr| Ok(expr.to_sql()?.sql(dialect)?))
            .collect::<Result<Vec<_>>>()?;

        let sql_aggr_expr_strs = aggr_expr
            .iter()
            .map(|expr| Ok(expr.to_sql_select()?.sql(dialect)?))
            .collect::<Result<Vec<_>>>()?;
        let aggr_csv = sql_aggr_expr_strs.join(", ");

        // Build new schema
        let mut new_schema_exprs = input_col_exprs.clone();
        new_schema_exprs.extend(aggr_expr.clone());
        let new_schema =
            make_new_schema_from_exprs(self.schema.as_ref(), new_schema_exprs.as_slice())?;

        if sql_group_expr_strs.is_empty() {
            self.chain_query_str(
                &format!(
                    "select {input_col_csv}, {new_col_csv} \
                    from {parent} \
                    CROSS JOIN (select {aggr_csv} from {parent}) as {inner_name}",
                    aggr_csv = aggr_csv,
                    parent = self.parent_name(),
                    input_col_csv = input_col_csv,
                    new_col_csv = new_col_csv,
                    inner_name = inner_name,
                ),
                new_schema,
            )
            .await
        } else {
            let group_by_csv = sql_group_expr_strs.join(", ");
            self.chain_query_str(
                &format!(
                    "select {input_col_csv}, {new_col_csv} \
                    from {parent} \
                    LEFT OUTER JOIN (select {aggr_csv}, {group_by_csv} from {parent} group by {group_by_csv}) as {inner_name} USING ({group_by_csv})",
                    aggr_csv = aggr_csv,
                    parent = self.parent_name(),
                    input_col_csv = input_col_csv,
                    new_col_csv = new_col_csv,
                    group_by_csv = group_by_csv,
                    inner_name = inner_name,
                ),
                new_schema
            ).await
        }
    }

    async fn filter(&self, predicate: Expr) -> Result<Arc<dyn DataFrame>> {
        let sql_predicate = predicate.to_sql()?;

        let query = Parser::parse_sql_query(&format!(
            "select * from {parent} where {sql_predicate}",
            parent = self.parent_name(),
            sql_predicate = sql_predicate.sql(&self.dialect)?,
        ))?;

        self.chain_query(query, self.schema.as_ref().clone())
            .await
            .with_context(|| format!("unsupported filter expression: {predicate}"))
    }

    async fn limit(&self, limit: i32) -> Result<Arc<dyn DataFrame>> {
        let query = Parser::parse_sql_query(&format!(
            "select * from {parent} LIMIT {limit}",
            parent = self.parent_name(),
            limit = limit
        ))?;

        self.chain_query(query, self.schema.as_ref().clone())
            .await
            .with_context(|| "unsupported limit query".to_string())
    }

    async fn fold(
        &self,
        fields: &[String],
        value_col: &str,
        key_col: &str,
        order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        let dialect = self.dialect();

        // Build selection that includes all input fields that aren't shadowed by key/value cols
        let input_selection = self
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name() == &key_col || f.name() == &value_col {
                    None
                } else {
                    Some(flat_col(f.name()))
                }
            })
            .collect::<Vec<_>>();

        // Build query per field
        let subquery_exprs = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                // Clone input selection and add key/val cols to it
                let mut subquery_selection = input_selection.clone();
                subquery_selection.push(lit(field).alias(key_col.clone()));
                if self.schema().column_with_name(field).is_some() {
                    // Field exists as a column in the parent table
                    subquery_selection.push(flat_col(field).alias(value_col.clone()));
                } else {
                    // Field does not exist in parent table, fill in NULL instead
                    subquery_selection.push(lit(ScalarValue::Null).alias(value_col.clone()));
                }

                if let Some(order_field) = order_field {
                    let field_order_col = format!("{order_field}_field");
                    subquery_selection.push(lit(i as u32).alias(field_order_col.clone()));
                }
                Ok(subquery_selection)
            })
            .collect::<Result<Vec<_>>>()?;

        let subqueries = subquery_exprs
            .iter()
            .map(|subquery_selection| {
                // Create selection CSV for subquery
                let selection_strs = subquery_selection
                    .iter()
                    .map(|sel| Ok(sel.to_sql_select()?.sql(dialect)?))
                    .collect::<Result<Vec<_>>>()?;
                let selection_csv = selection_strs.join(", ");

                Ok(format!(
                    "(SELECT {selection_csv} from {parent})",
                    selection_csv = selection_csv,
                    parent = self.parent_name()
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let union_subquery = subqueries.join(" UNION ALL ");
        let union_subquery_name = "_union";

        let mut selections = input_selection.clone();
        selections.push(flat_col(&key_col));
        selections.push(flat_col(&value_col));
        if let Some(order_field) = order_field {
            let field_order_col = format!("{order_field}_field");
            selections.push(flat_col(&field_order_col));
        }

        let selection_strs = selections
            .iter()
            .map(|sel| Ok(sel.to_sql_select()?.sql(dialect)?))
            .collect::<Result<Vec<_>>>()?;
        let selection_csv = selection_strs.join(", ");

        let sql =
            format!("SELECT {selection_csv} FROM ({union_subquery}) as {union_subquery_name}");

        let new_schmea =
            make_new_schema_from_exprs(self.schema.as_ref(), subquery_exprs[0].as_slice())?;
        let dataframe = self.chain_query_str(&sql, new_schmea).await?;

        if let Some(order_field) = order_field {
            // Add new ordering column, ordering by:
            // 1. input row ordering
            // 2. field index
            let field_order_col = format!("{order_field}_field");
            let order_col = Expr::WindowFunction(expr::WindowFunction {
                fun: window_function::WindowFunction::BuiltInWindowFunction(
                    BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by: vec![],
                order_by: vec![
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col(order_field)),
                        asc: true,
                        nulls_first: false,
                    }),
                    Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col(&field_order_col)),
                        asc: true,
                        nulls_first: false,
                    }),
                ],
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    end_bound: WindowFrameBound::CurrentRow,
                },
            })
            .alias(order_field);

            // Build output selections
            let mut selections = input_selection.clone();
            selections.push(flat_col(&key_col));
            selections.push(flat_col(&value_col));
            selections[0] = order_col;
            dataframe.select(selections).await
        } else {
            Ok(dataframe)
        }
    }

    async fn stack(
        &self,
        field: &str,
        orderby: Vec<Expr>,
        groupby: &[String],
        start_field: &str,
        stop_field: &str,
        mode: StackOffsetSpec,
    ) -> Result<Arc<dyn DataFrame>> {
        // Save off input columns
        let input_fields: Vec<_> = self
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let dialect = self.dialect();

        // Build partitioning column expressions
        let partition_by: Vec<_> = groupby.iter().map(|group| flat_col(group)).collect();

        let numeric_field = Expr::ScalarFunction {
            fun: BuiltinScalarFunction::Coalesce,
            args: vec![to_numeric(flat_col(field), &self.schema_df()?)?, lit(0)],
        };

        if let StackOffsetSpec::Zero = mode {
            // Build window expression
            let fun = WindowFunction::AggregateFunction(AggregateFunction::Sum);

            // Build window function to compute stacked value
            let window_expr = Expr::WindowFunction(expr::WindowFunction {
                fun,
                args: vec![numeric_field.clone()],
                partition_by,
                order_by: Vec::from(orderby),
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    end_bound: WindowFrameBound::CurrentRow,
                },
            })
            .alias(stop_field);

            let window_expr_str = window_expr.to_sql_select()?.sql(self.dialect())?;

            // For offset zero, we need to evaluate positive and negative field values separately,
            // then union the results. This is required to make sure stacks do not overlap. Negative
            // values stack in the negative direction and positive values stack in the positive
            // direction.
            let schema_exprs = vec![Expr::Wildcard, window_expr.clone()];
            let new_schema =
                make_new_schema_from_exprs(self.schema.as_ref(), schema_exprs.as_slice())?;

            let dataframe = self
                .chain_query_str(&format!(
                    "(SELECT *, {window_expr_str} from {parent} WHERE {numeric_field} >= 0) UNION ALL \
                                    (SELECT *, {window_expr_str} from {parent} WHERE {numeric_field} < 0)",
                    parent = self.parent_name(),
                    window_expr_str = window_expr_str,
                    numeric_field = numeric_field.to_sql()?.sql(self.dialect())?
                ),
                                 new_schema)
                .await?;

            // Build final selection
            let mut final_selection: Vec<_> = input_fields
                .iter()
                .filter_map(|field| {
                    if field == start_field || field == stop_field {
                        None
                    } else {
                        Some(flat_col(field))
                    }
                })
                .collect();

            // Compute start column by adding numeric field to stop column
            let start_col = flat_col(stop_field).sub(numeric_field).alias(start_field);
            final_selection.push(start_col);
            final_selection.push(flat_col(stop_field));

            Ok(dataframe.select(final_selection.clone()).await?)
        } else {
            // Center or Normalized stack modes

            // take absolute value of numeric field
            let numeric_field = abs(numeric_field);

            // Create __stack column with numeric field
            let stack_col_name = "__stack";
            let dataframe = self
                .select(vec![Expr::Wildcard, numeric_field.alias(stack_col_name)])
                .await?;

            let dataframe = dataframe
                .as_any()
                .downcast_ref::<SqlDataFrame>()
                .unwrap()
                .clone();

            // Create aggregate for total of stack value
            let total_agg = Expr::AggregateFunction(expr::AggregateFunction {
                fun: AggregateFunction::Sum,
                args: vec![flat_col(stack_col_name)],
                distinct: false,
                filter: None,
            })
            .alias("__total");
            let total_agg_str = total_agg.to_sql_select()?.sql(&dialect)?;

            // Add __total column with total or total per partition
            let schema_exprs = vec![Expr::Wildcard, total_agg.clone()];
            let new_schema =
                make_new_schema_from_exprs(&dataframe.schema(), schema_exprs.as_slice())?;

            let dataframe = if partition_by.is_empty() {
                dataframe
                    .chain_query_str(
                        &format!(
                            "SELECT * from {parent} CROSS JOIN (SELECT {total_agg_str} from {parent})",
                            parent = dataframe.parent_name(),
                            total_agg_str = total_agg_str,
                        ),
                        new_schema
                    ).await?
            } else {
                let partition_by_strs = partition_by
                    .iter()
                    .map(|p| Ok(p.to_sql()?.sql(self.dialect())?))
                    .collect::<Result<Vec<_>>>()?;
                let partition_by_csv = partition_by_strs.join(", ");

                dataframe.chain_query_str(
                    &format!(
                        "SELECT * FROM {parent} INNER JOIN \
                        (SELECT {partition_by_csv}, {total_agg_str} from {parent} GROUP BY {partition_by_csv}) as __inner \
                        USING ({partition_by_csv})",
                        parent = dataframe.parent_name(),
                        partition_by_csv = partition_by_csv,
                        total_agg_str = total_agg_str,
                    ),
                    new_schema
                ).await?
            };

            // Build window function to compute cumulative sum of stack column
            let fun = WindowFunction::AggregateFunction(AggregateFunction::Sum);
            let window_expr = Expr::WindowFunction(expr::WindowFunction {
                fun,
                args: vec![flat_col(stack_col_name)],
                partition_by,
                order_by: Vec::from(orderby),
                window_frame: WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    end_bound: WindowFrameBound::CurrentRow,
                },
            })
            .alias(stop_field);

            // Perform selection to add new field value
            let dataframe = dataframe.select(vec![Expr::Wildcard, window_expr]).await?;

            // Build final_selection
            let mut final_selection: Vec<_> = input_fields
                .iter()
                .filter_map(|field| {
                    if field == start_field || field == stop_field {
                        None
                    } else {
                        Some(flat_col(field))
                    }
                })
                .collect();

            // Now compute stop_field column by adding numeric field to start_field
            let dataframe = match mode {
                StackOffsetSpec::Center => {
                    let max_total = max(flat_col("__total")).alias("__max_total");
                    let max_total_str = max_total.to_sql_select()?.sql(&dialect)?;

                    // Compute new schema
                    let schema_exprs = vec![Expr::Wildcard, max_total.clone()];
                    let new_schema =
                        make_new_schema_from_exprs(&dataframe.schema(), schema_exprs.as_slice())?;

                    let sqldataframe = dataframe
                        .as_any()
                        .downcast_ref::<SqlDataFrame>()
                        .unwrap()
                        .clone();
                    let dataframe = sqldataframe
                        .chain_query_str(
                            &format!(
                                "SELECT * from {parent} CROSS JOIN (SELECT {max_total_str} from {parent})",
                                parent = sqldataframe.parent_name(),
                                max_total_str = max_total_str,
                            ),
                            new_schema
                        ).await?;

                    let first = flat_col("__max_total").sub(flat_col("__total")).div(lit(2));
                    let first_col = flat_col(stop_field).add(first);
                    let stop_col = first_col.clone().alias(stop_field);
                    let start_col = first_col.sub(flat_col(stack_col_name)).alias(start_field);
                    final_selection.push(start_col);
                    final_selection.push(stop_col);

                    dataframe
                }
                StackOffsetSpec::Normalize => {
                    let total_zero = flat_col("__total").eq(lit(0.0));

                    let start_col = when(total_zero.clone(), lit(0.0))
                        .otherwise(
                            flat_col(stop_field)
                                .sub(flat_col(stack_col_name))
                                .div(flat_col("__total")),
                        )?
                        .alias(start_field);

                    final_selection.push(start_col);

                    let stop_col = when(total_zero, lit(0.0))
                        .otherwise(flat_col(stop_field).div(flat_col("__total")))?
                        .alias(stop_field);

                    final_selection.push(stop_col);

                    dataframe
                }
                _ => return Err(VegaFusionError::internal("Unexpected stack mode")),
            };

            Ok(dataframe.select(final_selection.clone()).await?)
        }
    }

    async fn impute(
        &self,
        field: &str,
        value: ScalarValue,
        key: &str,
        groupby: &[String],
        order_field: Option<&str>,
    ) -> Result<Arc<dyn DataFrame>> {
        if groupby.is_empty() {
            // Value replacement for field with no groupby fields specified is equivalent to replacing
            // null values of that column with the fill value
            let select_columns: Vec<_> = self
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    let col_name = f.name();
                    if col_name == field {
                        Expr::ScalarFunction {
                            fun: BuiltinScalarFunction::Coalesce,
                            args: vec![flat_col(field), lit(value.clone())],
                        }
                        .alias(col_name)
                    } else {
                        flat_col(col_name)
                    }
                })
                .collect();

            self.select(select_columns).await
        } else {
            // Save off names of columns in the original input DataFrame
            let original_columns: Vec<_> = self
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();

            // First step is to build up a new DataFrame that contains the all possible combinations
            // of the `key` and `groupby` columns
            let key_col = flat_col(key);
            let key_col_str = key_col.to_sql_select()?.sql(self.dialect())?;

            let group_cols = groupby.iter().map(|c| flat_col(c)).collect::<Vec<_>>();
            let group_col_strs = group_cols
                .iter()
                .map(|c| Ok(c.to_sql_select()?.sql(self.dialect())?))
                .collect::<Result<Vec<_>>>()?;
            let group_cols_csv = group_col_strs.join(", ");

            // Build final selection
            // Finally, select all of the original DataFrame columns, filling in missing values
            // of the `field` columns
            let select_columns: Vec<_> = original_columns
                .iter()
                .map(|col_name| {
                    if col_name == field {
                        Expr::ScalarFunction {
                            fun: BuiltinScalarFunction::Coalesce,
                            args: vec![flat_col(field), lit(value.clone())],
                        }
                        .alias(col_name)
                    } else {
                        flat_col(col_name)
                    }
                })
                .collect();

            let select_column_strs = select_columns
                .iter()
                .map(|c| Ok(c.to_sql_select()?.sql(self.dialect())?))
                .collect::<Result<Vec<_>>>()?;

            let select_column_csv = select_column_strs.join(", ");

            let mut using_strs = vec![key_col_str.clone()];
            using_strs.extend(group_col_strs.clone());
            let using_csv = using_strs.join(", ");

            if let Some(order_field) = order_field {
                // Query with ordering column
                let sql = format!(
                    "SELECT {select_column_csv}, {order_field}_key, {order_field}_groups \
                     FROM (SELECT {key}, min({order_field}) as {order_field}_key from {parent} WHERE {key} IS NOT NULL GROUP BY {key}) AS _key \
                     CROSS JOIN (SELECT {group_cols_csv}, min({order_field}) as {order_field}_groups from {parent} GROUP BY {group_cols_csv}) as _groups \
                     LEFT OUTER JOIN {parent} \
                     USING ({using_csv})",
                    select_column_csv = select_column_csv,
                    group_cols_csv = group_cols_csv,
                    key = key_col_str,
                    using_csv = using_csv,
                    order_field = order_field,
                    parent = self.parent_name(),
                );

                let mut schema_exprs = select_columns.clone();
                schema_exprs.extend(vec![
                    min(flat_col(order_field)).alias(format!("{order_field}_key")),
                    min(flat_col(order_field)).alias(format!("{order_field}_groups")),
                ]);
                let new_schema =
                    make_new_schema_from_exprs(self.schema.as_ref(), schema_exprs.as_slice())?;
                let dataframe = self.chain_query_str(&sql, new_schema).await?;

                // Override ordering column since null values may have been introduced in the query above.
                // Match input ordering with imputed rows (those will null ordering column) pushed
                // to the end.
                let order_col = Expr::WindowFunction(expr::WindowFunction {
                    fun: window_function::WindowFunction::BuiltInWindowFunction(
                        BuiltInWindowFunction::RowNumber,
                    ),
                    args: vec![],
                    partition_by: vec![],
                    order_by: vec![
                        // Sort first by the original row order, pushing imputed rows to the end
                        Expr::Sort(expr::Sort {
                            expr: Box::new(flat_col(order_field)),
                            asc: true,
                            nulls_first: false,
                        }),
                        // Sort imputed rows by first row that resides group
                        // then by first row that matches a key
                        Expr::Sort(expr::Sort {
                            expr: Box::new(flat_col(&format!("{order_field}_groups"))),
                            asc: true,
                            nulls_first: false,
                        }),
                        Expr::Sort(expr::Sort {
                            expr: Box::new(flat_col(&format!("{order_field}_key"))),
                            asc: true,
                            nulls_first: false,
                        }),
                    ],
                    window_frame: WindowFrame {
                        units: WindowFrameUnits::Rows,
                        start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                        end_bound: WindowFrameBound::CurrentRow,
                    },
                })
                .alias(order_field);

                // Build vector of selections
                let mut selections = dataframe
                    .schema()
                    .fields
                    .iter()
                    .filter_map(|field| {
                        if field.name().starts_with(order_field) {
                            None
                        } else {
                            Some(flat_col(field.name()))
                        }
                    })
                    .collect::<Vec<_>>();
                selections.insert(0, order_col);

                dataframe.select(selections).await
            } else {
                // Impute query without ordering column
                let sql = format!(
                    "SELECT {select_column_csv}  \
                     FROM (SELECT {key} from {parent} WHERE {key} IS NOT NULL GROUP BY {key}) AS _key \
                     CROSS JOIN (SELECT {group_cols_csv} from {parent} GROUP BY {group_cols_csv}) as _groups \
                     LEFT OUTER JOIN {parent} \
                     USING ({using_csv})",
                    select_column_csv = select_column_csv,
                    group_cols_csv = group_cols_csv,
                    key = key_col_str,
                    using_csv = using_csv,
                    parent = self.parent_name(),
                );
                let new_schema =
                    make_new_schema_from_exprs(self.schema.as_ref(), select_columns.as_slice())?;
                self.chain_query_str(&sql, new_schema).await
            }
        }
    }
}

impl SqlDataFrame {
    pub async fn try_new(conn: Arc<dyn SqlConnection>, table: &str) -> Result<Self> {
        let tables = conn.tables().await?;
        let schema = tables
            .get(table)
            .cloned()
            .with_context(|| format!("Connection has no table named {table}"))?;
        // Should quote column names
        let columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();
        let select_items = columns.join(", ");

        let query = Parser::parse_sql_query(&format!("select {select_items} from {table}"))?;

        Ok(Self {
            prefix: format!("{table}_"),
            ctes: vec![query],
            schema: Arc::new(schema.clone()),
            conn,
            dialect: Arc::new(make_datafusion_dialect()),
        })
    }

    pub fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    pub fn parent_name(&self) -> String {
        parent_cte_name_for_index(&self.prefix, self.ctes.len())
    }

    pub fn as_query(&self) -> Query {
        query_chain_to_cte(self.ctes.as_slice(), &self.prefix)
    }

    async fn chain_query_str(&self, query: &str, new_schema: Schema) -> Result<Arc<dyn DataFrame>> {
        // println!("chain_query_str: {}", query);
        let query_ast = Parser::parse_sql_query(query)?;
        self.chain_query(query_ast, new_schema).await
    }

    async fn chain_query(&self, query: Query, new_schema: Schema) -> Result<Arc<dyn DataFrame>> {
        let mut new_ctes = self.ctes.clone();
        new_ctes.push(query);

        Ok(Arc::new(SqlDataFrame {
            prefix: self.prefix.clone(),
            schema: Arc::new(new_schema),
            ctes: new_ctes,
            conn: self.conn.clone(),
            dialect: self.dialect.clone(),
        }))
    }

    fn make_select_star(&self) -> Query {
        Parser::parse_sql_query(&format!(
            "select * from {parent}",
            parent = self.parent_name()
        ))
        .unwrap()
    }
}

fn make_new_schema_from_exprs(schema: &Schema, exprs: &[Expr]) -> Result<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    for expr in exprs {
        if let Expr::Wildcard = expr {
            // Add field for each input schema field
            fields.extend(schema.fields().clone())
        } else {
            // Add field for expression
            let schema_df = DFSchema::try_from(schema.clone())?;
            let dtype = expr.get_type(&schema_df)?;
            let name = expr.display_name()?;
            fields.push(Field::new(name, dtype, true));
        }
    }

    let new_schema = Schema::new(fields);
    Ok(new_schema)
}

fn cte_name_for_index(prefix: &str, index: usize) -> String {
    format!("{prefix}{index}")
}

fn parent_cte_name_for_index(prefix: &str, index: usize) -> String {
    cte_name_for_index(prefix, index - 1)
}

fn query_chain_to_cte(queries: &[Query], prefix: &str) -> Query {
    // Build vector of CTE AST nodes for all but the last query
    let cte_tables: Vec<_> = queries[..queries.len() - 1]
        .iter()
        .enumerate()
        .map(|(i, query)| {
            let this_cte_name = cte_name_for_index(prefix, i);
            Cte {
                alias: TableAlias {
                    name: Ident {
                        value: this_cte_name,
                        quote_style: None,
                    },
                    columns: vec![],
                },
                query: query.clone(),
                from: None,
            }
        })
        .collect();

    // The final query becomes the top-level query with CTEs attached to it
    let mut final_query = queries[queries.len() - 1].clone();
    final_query.with = if cte_tables.is_empty() {
        None
    } else {
        Some(With {
            recursive: false,
            cte_tables,
        })
    };

    final_query
}
