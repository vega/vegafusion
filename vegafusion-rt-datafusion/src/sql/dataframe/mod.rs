use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use crate::sql::connection::SqlConnection;
use datafusion::prelude::{SessionContext, Expr as DfExpr};
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::ast::{
    Query, TableAlias
};
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlgen::parser::Parser;
use std::sync::Arc;
use datafusion_expr::Expr;
use vegafusion_core::arrow::datatypes::{Schema, SchemaRef};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext};
use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::order::ToSqlOrderByExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::compile::window::ToSqlWindowFunction;

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
            if matches!(expr, Expr::WindowFunction {..}) {
                Ok(expr.to_sql_window()?.sql(&dialect)?)
            } else {
                Ok(expr.to_sql_select()?.sql(&dialect)?)
            }
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
    use crate::sql::connection::sqlite_conn::SqLiteConnection;
    use crate::sql::dataframe::SqlDataFrame;
    use crate::sql::connection::datafusion_conn::DataFusionConnection;
    use crate::data::table::VegaFusionTableUtils;

    use std::ops::Mul;
    use sqlgen::dialect::{Dialect, DialectDisplay};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{BuiltInWindowFunction, col, Expr, lit, max, WindowFunction};

    fn crate_dir() -> String {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn try_it() {
        let conn = SqLiteConnection::try_new(
            &format!("{}/tests/data/vega_datasets.db", crate_dir())
        ).await.unwrap();

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
        let conn = SqLiteConnection::try_new(
            &format!("{}/tests/data/vega_datasets.db", crate_dir())
        ).await.unwrap();

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

    #[tokio::test]
    async fn try_it3() {
        let conn = SqLiteConnection::try_new(
            &format!("{}/tests/data/vega_datasets.db", crate_dir())
        ).await.unwrap();

        let df = SqlDataFrame::try_new(Arc::new(conn), "stock").await.unwrap();

        let df = df.select(vec![
            Expr::Wildcard,
            Expr::WindowFunction {
                fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                window_frame: None
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
    async fn try_datafusion_connection() {
        let ctx = SessionContext::new();
        let stock_path = format!("{}/tests/data/stock.csv", crate_dir());
        ctx.register_csv("stock",  &stock_path, Default::default()).await.unwrap();

        let conn = DataFusionConnection::new(Arc::new(ctx));
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

        let s = df.as_query().sql(&Dialect::datafusion()).unwrap();
        println!("datafusion: {}", s);

        let table = df.collect().await.unwrap();
        println!("{}", table.pretty_format(Some(10)).unwrap());
    }
}
