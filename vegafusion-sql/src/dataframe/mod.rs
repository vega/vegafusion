use crate::connection::SqlConnection;
use datafusion::prelude::{SessionContext, Expr as DfExpr};
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::ast::{
    OrderByExpr, Query, SetExpr, TableAlias, TableFactor, TableWithJoins,
    Select
};
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlgen::parser::Parser;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::{Schema, SchemaRef};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use crate::compile::expr::ToSqlExpr;
use crate::compile::order::ToSqlOrderByExpr;
use crate::compile::select::ToSqlSelectItem;

#[derive(Clone)]
pub struct SqlDataFrame {
    prefix: String,
    schema: SchemaRef,
    ctes: Vec<Query>,
    conn: Arc<dyn SqlConnection>,
    session_context: Arc<SessionContext>,
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
        let query =
            Parser::parse_sql_query(&format!("select {} from {}", select_items, table))?;

        Ok(Self {
            prefix: format!("{}_", table),
            ctes: vec![query],
            schema: Arc::new(schema.clone()),
            session_context: Arc::new(conn.session_context().await?),
            conn,
        })
    }

    pub fn parent_name(&self) -> String {
        parent_cte_name_for_index(&self.prefix, self.ctes.len())
    }

    pub fn as_query(&self) -> Query {
        query_chain_to_cte(self.ctes.as_slice(), &self.prefix)
    }

    pub fn chain_query_str(&self, query: &str) -> Result<Arc<Self>> {
        let query_ast = Parser::parse_sql_query(query)?;
        self.chain_query(query_ast)
    }

    pub fn chain_query(&self, query: Query) -> Result<Arc<Self>> {
        let mut new_ctes = self.ctes.clone();
        new_ctes.push(query);

        let combined_query = query_chain_to_cte(new_ctes.as_slice(), &self.prefix);

        // First, convert the combined query to a string using the connection's dialect to make
        // sure that it is supported by the connection
        combined_query.sql(self.conn.dialect())?;

        // Now convert to string in the DataFusion dialect for schema inference
        let dialect = Dialect::datafusion();
        let query_str = combined_query.sql(&dialect)?;
        let logical_plan = self.session_context.create_logical_plan(&query_str)?;
        let new_schema: Schema = logical_plan.schema().as_ref().into();

        Ok(Arc::new(SqlDataFrame {
            prefix: self.prefix.clone(),
            schema: Arc::new(new_schema),
            ctes: new_ctes,
            conn: self.conn.clone(),
            session_context: self.session_context.clone(),
        }))
    }

    pub async fn collect(&self) -> Result<VegaFusionTable> {
        let query_string = self.as_query().sql(self.conn.dialect())?;
        self.conn.fetch_query(&query_string, &self.schema).await
    }

    pub fn sort(&self, expr: Vec<DfExpr>) -> Result<Arc<Self>> {
        let mut query = self.make_select_star();
        let sql_exprs = expr.iter().map(|expr| expr.to_sql_order()).collect::<Result<Vec<_>>>()?;
        query.order_by = sql_exprs;
        self.chain_query(query)
    }

    pub fn select(&self, expr: Vec<DfExpr>) -> Result<Arc<Self>> {
        let dialect = Dialect::datafusion();
        let sql_expr_strs = expr.iter().map(|expr|
            Ok(expr.to_sql_select()?.sql(&dialect)?)
        ).collect::<Result<Vec<_>>>()?;

        let select_csv = sql_expr_strs.join(", ");
        let query = Parser::parse_sql_query(&format!(
            "select {select_csv} from {parent}",
            select_csv=select_csv,
            parent=self.parent_name()
        ))?;

        self.chain_query(query)
    }

    pub fn aggregate(&self, group_expr: Vec<DfExpr>, aggr_expr: Vec<DfExpr>) -> Result<Arc<Self>> {
        let dialect = Dialect::datafusion();
        let sql_group_expr_strs = group_expr.iter().map(|expr| {
            Ok(expr.to_sql()?.sql(&dialect)?)
        }).collect::<Result<Vec<_>>>()?;

        let mut sql_aggr_expr_strs = aggr_expr.iter().map(|expr| {
            Ok(expr.to_sql_select()?.sql(&dialect)?)
        }).collect::<Result<Vec<_>>>()?;

        // Add group exprs to selection
        sql_aggr_expr_strs.extend(sql_group_expr_strs.clone());

        let group_by_csv = sql_group_expr_strs.join(", ");
        let aggr_csv = sql_aggr_expr_strs.join(", ");

        let query = Parser::parse_sql_query(&format!(
            "select {aggr_csv} from {parent} group by {group_by_csv}",
            aggr_csv=aggr_csv,
            parent=self.parent_name(),
            group_by_csv=group_by_csv
        ))?;

        self.chain_query(query)
    }

    fn make_select_star(&self) -> Query {
        Parser::parse_sql_query(&format!(
            "select * from {parent}", parent=self.parent_name()
        )).unwrap()
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

#[cfg(test)]
mod test {
    use std::ops::Mul;
    use crate::connection::sqlite::SqLiteConnection;
    use crate::dataframe::SqlDataFrame;
    use sqlgen::dialect::{Dialect, DialectDisplay};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use datafusion_expr::{col, Expr, lit, max};
    use vegafusion_core::arrow::datatypes::{DataType, Field, Schema};
    use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;

    #[tokio::test]
    async fn try_it() {
        let pool = SqlitePool::connect(&"/media/jmmease/SSD2/rustDev/vega-fusion/vega-fusion/vegafusion-sql/tests/data/vega_datasets.db")
            .await
            .unwrap();

        let conn = SqLiteConnection::new(Arc::new(pool));

        let df = SqlDataFrame::try_new(Arc::new(conn), "stock").await.unwrap();
        println!("{:#?}", df.schema);

        let df = df.select(vec![
            col("date"),
            col("symbol"),
            col("price"),
            col("price").mul(lit(2)).alias("dbl_price")
        ]).unwrap();

        let df = df.sort(vec![
            Expr::Sort {
                expr: Box::new(col("price")),
                asc: false,
                nulls_first: true
            }
        ]).unwrap();

        // Extract SQL in the sqlite dialect
        let s = df.as_query().sql(&Dialect::sqlite()).unwrap();
        println!("sqlite: {}", s);

        let s = df.as_query().sql(&Dialect::datafusion()).unwrap();
        println!("datafusion: {}", s);

        let table = df.collect().await.unwrap();
        println!("{}", table.pretty_format(Some(10)).unwrap());
    }

    #[tokio::test]
    async fn try_it2() {
        let pool = SqlitePool::connect(&"/media/jmmease/SSD2/rustDev/vega-fusion/vega-fusion/vegafusion-sql/tests/data/vega_datasets.db")
            .await
            .unwrap();

        let conn = SqLiteConnection::new(Arc::new(pool));

        let df = SqlDataFrame::try_new(Arc::new(conn), "stock").await.unwrap();

        let df = df.select(vec![
            col("date"),
            col("symbol"),
            col("price"),
            col("price").mul(lit(2)).alias("dbl_price")
        ]).unwrap();

        let df = df.aggregate(
            vec![col("symbol")],
            vec![max(col("dbl_price")).alias("max_dbl_price")]
        ).unwrap();

        let df = df.sort(vec![Expr::Sort {
            expr: Box::new(col("max_dbl_price")),
            asc: false,
            nulls_first: true
        }]).unwrap();

        println!("{:#?}", df.schema);

        // Extract SQL in the sqlite dialect
        let s = df.as_query().sql(&Dialect::sqlite()).unwrap();
        println!("sqlite: {}", s);

        let s = df.as_query().sql(&Dialect::datafusion()).unwrap();
        println!("datafusion: {}", s);

        let table = df.collect().await.unwrap();
        println!("{}", table.pretty_format(Some(10)).unwrap());
    }
}
