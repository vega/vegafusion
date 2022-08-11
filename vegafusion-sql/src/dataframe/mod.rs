use crate::connection::SqlDatabaseConnection;
use datafusion::prelude::SessionContext;
use sqlgen::ast::Expr;
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::ast::{OrderByExpr, Query, SetExpr, TableAlias, TableFactor, TableWithJoins};
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlgen::parser::Parser;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::{Schema, SchemaRef};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

#[derive(Clone)]
pub struct SqlDataFrame {
    prefix: String,
    schema: SchemaRef,
    ctes: Vec<Query>,
    conn: Arc<dyn SqlDatabaseConnection>,
    session_context: Arc<SessionContext>,
}

impl SqlDataFrame {
    pub async fn try_new(conn: Arc<dyn SqlDatabaseConnection>, table: &str) -> Result<Self> {
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
            Parser::parse_sql_query(&format!("select {} from {}", select_items, table)).unwrap();

        Ok(Self {
            prefix: "t_".to_string(),
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

    fn chain_query_str(&self, query: &str) -> Result<Arc<Self>> {
        let query_ast = Parser::parse_sql_query(query)
            .map_err(|err| VegaFusionError::internal(err.to_string()))?;
        self.chain_query(query_ast)
    }

    fn chain_query(&self, query: Query) -> Result<Arc<Self>> {
        let mut new_ctes = self.ctes.clone();
        new_ctes.push(query);

        let combined_query = query_chain_to_cte(new_ctes.as_slice(), &self.prefix);

        // First, convert the combined query to a string using the connection's dialect to make
        // sure that it is supported by the connection
        combined_query.sql(self.conn.dialect())?;

        // Now convert to string in the DataFusion dialect for schema inference
        let query_str = combined_query.sql(&Dialect::datafusion())?;
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

    async fn collect(&self) -> Result<VegaFusionTable> {
        let query_string = self.as_query().sql(self.conn.dialect())?;
        self.conn.fetch_query(&query_string, &self.schema).await
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
    use crate::connection::sqlite::SqLiteConnection;
    use crate::dataframe::SqlDataFrame;
    use sqlgen::dialect::{Dialect, DialectDisplay};
    use sqlx::SqlitePool;
    use std::sync::Arc;
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

        // Transform query
        let parent = df.parent_name();
        let df = df
            .chain_query_str(&format!(
                "select date, symbol, price, price * 2 as dbl_price from {parent}",
                parent = df.parent_name()
            ))
            .unwrap();
        println!("{:#?}", df.schema);

        let df = df
            .chain_query_str(&format!(
                "select * from {parent} order by date",
                parent = df.parent_name()
            ))
            .unwrap();

        // Extract SQL in the sqlite dialect
        let s = df.as_query().sql(&Dialect::sqlite()).unwrap();
        println!("{}", s);

        let table = df.collect().await.unwrap();
        println!("{}", table.pretty_format(Some(10)).unwrap());
    }
}
