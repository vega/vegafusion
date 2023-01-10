use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::{cast_to, data_type, is_string_datatype};
use crate::expression::escape::{flat_col, unescaped_col};
use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::aggregate::make_aggr_expr;
use crate::transform::utils::RecordBatchUtils;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::Column;
use datafusion_expr::{coalesce, col, expr::Sort, lit, min, when, Expr};
use sqlgen::dialect::DialectDisplay;
use std::sync::Arc;
use vegafusion_core::arrow::array::StringArray;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::data::ORDER_COL;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{AggregateOp, Pivot};
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Pivot {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        // Make sure the pivot column is a string
        let pivot_dtype = data_type(&unescaped_col(&self.field), &dataframe.schema_df())?;
        let dataframe = if matches!(pivot_dtype, DataType::Boolean) {
            // Boolean column type. For consistency with vega, replace 0 with "false" and 1 with "true"
            let select_exprs: Vec<_> = dataframe
                .schema()
                .fields
                .iter()
                .map(|field| {
                    if field.name() == &self.field {
                        Ok(when(col(&self.field).eq(lit(true)), lit("true"))
                            .otherwise(lit("false"))
                            .expect("Failed to construct Case expression")
                            .alias(&self.field))
                    } else {
                        Ok(unescaped_col(field.name()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            dataframe.select(select_exprs).await?
        } else if !is_string_datatype(&pivot_dtype) {
            // Column type is not string, so cast values to strings
            let select_exprs: Vec<_> = dataframe
                .schema()
                .fields
                .iter()
                .map(|field| {
                    if field.name() == &self.field {
                        Ok(cast_to(
                            unescaped_col(&self.field),
                            &DataType::Utf8,
                            &dataframe.schema_df(),
                        )?
                        .alias(&self.field))
                    } else {
                        Ok(unescaped_col(field.name()))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            dataframe.select(select_exprs).await?
        } else {
            // Column type is string
            dataframe
        };

        if self.groupby.is_empty() {
            pivot_without_grouping(self, dataframe).await
        } else {
            pivot_with_grouping(self, dataframe).await
        }
    }
}

async fn extract_sorted_pivot_values(tx: &Pivot, dataframe: &SqlDataFrame) -> Result<Vec<String>> {
    let agg_query = dataframe
        .aggregate(vec![unescaped_col(&tx.field)], vec![])
        .await?;

    let limit = match tx.limit {
        None | Some(0) => None,
        Some(i) => Some(i),
    };

    let sorted_query = agg_query
        .sort(
            vec![Expr::Sort(Sort {
                expr: Box::new(Expr::Column(Column {
                    relation: Some(agg_query.parent_name()),
                    name: tx.field.clone(),
                })),
                asc: true,
                nulls_first: false,
            })],
            limit,
        )
        .await?;

    let pivot_result = sorted_query.collect().await?;
    let pivot_batch = pivot_result.to_record_batch()?;
    let pivot_array = pivot_batch.column_by_name(&tx.field)?;
    let pivot_array = pivot_array
        .as_any()
        .downcast_ref::<StringArray>()
        .with_context(|| "Failed to downcast pivot column to String")?;
    let pivot_vec: Vec<_> = pivot_array
        .iter()
        .filter_map(|val| val.map(|s| s.to_string()))
        .collect();
    Ok(pivot_vec)
}

async fn pivot_without_grouping(
    tx: &Pivot,
    dataframe: Arc<SqlDataFrame>,
) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
    let pivot_vec = extract_sorted_pivot_values(tx, &dataframe).await?;

    if pivot_vec.is_empty() {
        return Err(VegaFusionError::internal("Unexpected empty pivot dataset"));
    }

    // Process aggregate operation
    let agg_op: AggregateOp = tx
        .op
        .map(|op_code| AggregateOp::from_i32(op_code).unwrap())
        .unwrap_or(AggregateOp::Sum);
    let fill_zero = should_fill_zero(&agg_op);

    // Extract Sql Dialect that we can use to safely convert DataFusion expressions to SQL strings
    let dialect = dataframe.dialect();

    // Initialize vector of final selections
    let mut final_selections: Vec<_> = Vec::new();

    // Build a vector of subqueries to be cross joined together
    let mut subqueries = Vec::new();
    for pivot_val in pivot_vec.iter() {
        // Build aggregate expression string
        let agg_expr = make_aggr_expr(Some(tx.value.clone()), &agg_op, &dataframe.schema_df())?;
        let agg = agg_expr.alias(pivot_val).to_sql_select()?.sql(dialect)?;

        // Build predicate expression string
        let predicate_expr = unescaped_col(&tx.field).eq(lit(pivot_val.as_str()));
        let predicate = predicate_expr.to_sql()?.sql(dialect)?;

        // Build subquery
        let subquery = format!(
            "SELECT {agg} FROM {parent_name} WHERE {predicate}",
            agg = agg,
            parent_name = dataframe.parent_name(),
            predicate = predicate,
        );
        subqueries.push(subquery);

        // Add column to final selections
        let select_expr = if fill_zero {
            coalesce(vec![flat_col(pivot_val), lit(0)]).alias(pivot_val)
        } else {
            flat_col(pivot_val)
        };
        final_selections.push(select_expr)
    }

    // Query will result in a single row, so add a constant valued ORDER_COL
    final_selections.insert(0, lit(0u32).alias(ORDER_COL));

    // Build final query
    let final_selection_strs: Vec<_> = final_selections
        .iter()
        .map(|sel| sel.to_sql_select().unwrap().sql(dialect).unwrap())
        .collect();
    let selection_csv = final_selection_strs.join(", ");
    let mut query_str = format!(
        "SELECT {selection_csv} FROM \n({subquery}) as _pivot_0\n",
        selection_csv = selection_csv,
        subquery = subqueries[0]
    );
    for (i, subquery) in subqueries.iter().enumerate().skip(1) {
        // Extend query with join
        let subquery_name = format!("_pivot_{i}", i = i);
        query_str.push_str(&format!(
            "CROSS JOIN ({subquery}) as {subquery_name} \n",
            subquery = subquery,
            subquery_name = subquery_name,
        ));
    }

    let dataframe_joined = dataframe.chain_query_str(&query_str).await?;

    // Perform final selection
    let selected = dataframe_joined.select(final_selections).await?;

    Ok((selected, Vec::new()))
}

async fn pivot_with_grouping(
    tx: &Pivot,
    dataframe: Arc<SqlDataFrame>,
) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
    let pivot_vec = extract_sorted_pivot_values(tx, &dataframe).await?;

    if pivot_vec.is_empty() {
        return Err(VegaFusionError::internal("Unexpected empty pivot dataset"));
    }

    // Process aggregate operation
    let agg_op: AggregateOp = tx
        .op
        .map(|op_code| AggregateOp::from_i32(op_code).unwrap())
        .unwrap_or(AggregateOp::Sum);
    let fill_zero = should_fill_zero(&agg_op);

    // Extract Sql Dialect that we can use to safely convert DataFusion expressions to SQL strings
    let dialect = dataframe.dialect();

    // Create dataframe containing the unique group values
    let groupby_cols: Vec<_> = tx
        .groupby
        .iter()
        .map(|field| unescaped_col(field))
        .collect();
    let groupby_strs: Vec<_> = groupby_cols
        .iter()
        .map(|col| col.to_sql().unwrap().sql(dialect).unwrap())
        .collect();
    let groupby_csv = groupby_strs.join(", ");

    let grouped_dataframe = dataframe
        .aggregate(
            groupby_cols,
            vec![min(flat_col(ORDER_COL)).alias(ORDER_COL)],
        )
        .await?;

    // Save off parent table names
    let dataframe_parent_name = dataframe.parent_name();
    let grouped_parent_name = grouped_dataframe.parent_name();

    // Initialize vector of final selections
    let mut final_selections: Vec<_> = tx.groupby.iter().map(|c| unescaped_col(c)).collect();
    final_selections.insert(0, flat_col(ORDER_COL));

    // Initialize empty query string
    let mut query_str = String::new();

    for (i, pivot_val) in pivot_vec.iter().enumerate() {
        // Build aggregate expression string
        let agg_expr = make_aggr_expr(Some(tx.value.clone()), &agg_op, &dataframe.schema_df())?;
        let agg = agg_expr.alias(pivot_val).to_sql_select()?.sql(dialect)?;

        // Build predicate expression string
        let predicate_expr = unescaped_col(&tx.field).eq(lit(pivot_val.as_str()));
        let predicate = predicate_expr.to_sql()?.sql(dialect)?;

        // Build subquery
        let subquery = format!(
            "SELECT {groupby_csv}, {agg} FROM {parent_name} WHERE {predicate} GROUP BY {groupby_csv}",
            groupby_csv = groupby_csv,
            agg = agg,
            parent_name = dataframe_parent_name,
            predicate = predicate,
        );

        // Extend query with join
        let subquery_name = format!("_pivot_{i}", i = i);
        query_str.push_str(&format!(
            "LEFT OUTER JOIN ({subquery}) as {subquery_name} USING ({groupby_csv}) ",
            subquery = subquery,
            subquery_name = subquery_name,
            groupby_csv = groupby_csv,
        ));

        // Add column to final selections
        let select_expr = if fill_zero {
            coalesce(vec![flat_col(pivot_val), lit(0)]).alias(pivot_val)
        } else {
            flat_col(pivot_val)
        };
        final_selections.push(select_expr)
    }

    // Prepend the initial selection
    let final_selection_strs: Vec<_> = final_selections
        .iter()
        .map(|sel| sel.to_sql_select().unwrap().sql(dialect).unwrap())
        .collect();
    let selection_csv = final_selection_strs.join(", ");
    query_str.insert_str(
        0,
        &format!(
            "SELECT {selection_csv} FROM \"{parent_name}\"\n",
            selection_csv = selection_csv,
            parent_name = grouped_parent_name
        ),
    );

    // Perform query and apply final selections
    let dataframe_joined = grouped_dataframe.chain_query_str(&query_str).await?;
    let selected = dataframe_joined.select(final_selections).await?;

    Ok((selected, Vec::new()))
}

/// Test whether null values should be replaced by zero for the specified aggregation
fn should_fill_zero(op: &AggregateOp) -> bool {
    matches!(op, AggregateOp::Count | AggregateOp::Sum)
}
