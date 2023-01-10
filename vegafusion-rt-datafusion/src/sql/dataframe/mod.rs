use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::order::ToSqlOrderByExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::connection::SqlConnection;
use datafusion::common::DFSchema;
use datafusion::prelude::{Expr as DfExpr, SessionContext};
use datafusion_expr::{lit, Expr};
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::ast::{Query, TableAlias};
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlgen::parser::Parser;
use std::collections::hash_map::DefaultHasher;

use crate::sql::connection::datafusion_conn::make_datafusion_dialect;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::{Schema, SchemaRef};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

#[derive(Clone)]
pub struct SqlDataFrame {
    prefix: String,
    schema: SchemaRef,
    ctes: Vec<Query>,
    conn: Arc<dyn SqlConnection>,
    session_context: Arc<SessionContext>,
    dialect: Arc<Dialect>,
}

impl SqlDataFrame {
    pub async fn try_new(conn: Arc<dyn SqlConnection>, table: &str) -> Result<Self> {
        let tables = conn.tables().await?;
        let schema = tables
            .get(table)
            .cloned()
            .with_context(|| format!("Connection has no table named {}", table))?;
        // Should quote column names
        let columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
            .collect();
        let select_items = columns.join(", ");

        let query = Parser::parse_sql_query(&format!("select {} from {}", select_items, table))?;

        Ok(Self {
            prefix: format!("{}_", table),
            ctes: vec![query],
            schema: Arc::new(schema.clone()),
            session_context: Arc::new(conn.session_context().await?),
            conn,
            dialect: Arc::new(make_datafusion_dialect()),
        })
    }

    pub fn schema(&self) -> Schema {
        self.schema.as_ref().clone()
    }

    pub fn schema_df(&self) -> DFSchema {
        DFSchema::try_from(self.schema.as_ref().clone()).unwrap()
    }

    pub fn dialect(&self) -> &Dialect {
        &self.dialect
    }

    pub fn fingerprint(&self) -> u64 {
        let mut hasher = deterministic_hash::DeterministicHasher::new(DefaultHasher::new());

        // Add connection id in hash
        self.conn.id().hash(&mut hasher);

        // Add query to hash
        let query_str = self.as_query().sql(self.conn.dialect()).unwrap();
        query_str.hash(&mut hasher);

        hasher.finish()
    }

    pub fn parent_name(&self) -> String {
        parent_cte_name_for_index(&self.prefix, self.ctes.len())
    }

    pub fn as_query(&self) -> Query {
        query_chain_to_cte(self.ctes.as_slice(), &self.prefix)
    }

    pub async fn chain_query_str(&self, query: &str) -> Result<Arc<Self>> {
        // println!("chain_query_str: {}", query);
        let query_ast = Parser::parse_sql_query(query)?;
        self.chain_query(query_ast).await
    }

    pub async fn chain_query(&self, query: Query) -> Result<Arc<Self>> {
        let mut new_ctes = self.ctes.clone();
        new_ctes.push(query);

        let combined_query = query_chain_to_cte(new_ctes.as_slice(), &self.prefix);

        // First, convert the combined query to a string using the connection's dialect to make
        // sure that it is supported by the connection
        combined_query
            .sql(self.conn.dialect())
            .map_err(|err| VegaFusionError::sql_not_supported(err.to_string()))?;

        // Now convert to string in the DataFusion dialect for schema inference
        let query_str = combined_query.sql(&self.dialect)?;
        // println!("datafusion: {}", query_str);

        let logical_plan = self
            .session_context
            .state()
            .create_logical_plan(&query_str)
            .await?;

        // println!("logical_plan: {:?}", logical_plan);
        let new_schema: Schema = logical_plan.schema().as_ref().into();

        Ok(Arc::new(SqlDataFrame {
            prefix: self.prefix.clone(),
            schema: Arc::new(new_schema),
            ctes: new_ctes,
            conn: self.conn.clone(),
            session_context: self.session_context.clone(),
            dialect: self.dialect.clone(),
        }))
    }

    pub async fn collect(&self) -> Result<VegaFusionTable> {
        let query_string = self.as_query().sql(self.conn.dialect())?;
        self.conn.fetch_query(&query_string, &self.schema).await
    }

    pub async fn sort(&self, expr: Vec<DfExpr>, limit: Option<i32>) -> Result<Arc<Self>> {
        let mut query = self.make_select_star();
        let sql_exprs = expr
            .iter()
            .map(|expr| expr.to_sql_order())
            .collect::<Result<Vec<_>>>()?;
        query.order_by = sql_exprs;
        if let Some(limit) = limit {
            query.limit = Some(lit(limit).to_sql().unwrap())
        }
        self.chain_query(query).await
    }

    pub async fn select(&self, expr: Vec<DfExpr>) -> Result<Arc<Self>> {
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

        self.chain_query(query).await
    }

    pub async fn aggregate(
        &self,
        group_expr: Vec<DfExpr>,
        aggr_expr: Vec<DfExpr>,
    ) -> Result<Arc<Self>> {
        let sql_group_expr_strs = group_expr
            .iter()
            .map(|expr| Ok(expr.to_sql()?.sql(&self.dialect)?))
            .collect::<Result<Vec<_>>>()?;

        let mut sql_aggr_expr_strs = aggr_expr
            .iter()
            .map(|expr| Ok(expr.to_sql_select()?.sql(&self.dialect)?))
            .collect::<Result<Vec<_>>>()?;

        // Add group exprs to selection
        sql_aggr_expr_strs.extend(sql_group_expr_strs.clone());
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

        self.chain_query(query).await
    }

    pub async fn filter(&self, predicate: Expr) -> Result<Arc<Self>> {
        let sql_predicate = predicate.to_sql()?;

        let query = Parser::parse_sql_query(&format!(
            "select * from {parent} where {sql_predicate}",
            parent = self.parent_name(),
            sql_predicate = sql_predicate.sql(&self.dialect)?,
        ))?;

        self.chain_query(query)
            .await
            .with_context(|| format!("unsupported filter expression: {}", predicate))
    }

    pub async fn limit(&self, limit: i32) -> Result<Arc<Self>> {
        let query = Parser::parse_sql_query(&format!(
            "select * from {parent} LIMIT {limit}",
            parent = self.parent_name(),
            limit = limit
        ))?;

        self.chain_query(query)
            .await
            .with_context(|| "unsupported limit query".to_string())
    }

    fn make_select_star(&self) -> Query {
        Parser::parse_sql_query(&format!(
            "select * from {parent}",
            parent = self.parent_name()
        ))
        .unwrap()
    }
}

fn cte_name_for_index(prefix: &str, index: usize) -> String {
    format!("{}{}", prefix, index)
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
