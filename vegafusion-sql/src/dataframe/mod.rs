use std::sync::Arc;
use datafusion::prelude::SessionContext;
use vegafusion_core::arrow::datatypes::{Schema, SchemaRef};
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use sqlgen::ast::{OrderByExpr, Query, SetExpr, TableAlias, TableFactor, TableWithJoins};
use sqlgen::ast::Expr;
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::dialect::{Dialect, DialectDisplay};
use sqlgen::parser::Parser;
use crate::connection::SqlDatabaseConnection;


#[derive(Clone)]
pub struct SqlDataFrame {
    prefix: String,
    schema: SchemaRef,
    ctes: Vec<Query>,
    conn: Arc<dyn SqlDatabaseConnection>,
    session_context: Arc<SessionContext>
}

impl SqlDataFrame {
    pub fn try_new(conn: Arc<dyn SqlDatabaseConnection>, table: &str) -> Result<Self> {
        let tables = conn.tables()?;
        let schema = tables.get(table).cloned().with_context(
            || format!("Connection has no table named {}", table)
        )?;
        // Should quote column names
        let columns: Vec<_> = schema.fields().iter().map(|f| format!("\"{}\"", f.name())).collect();
        let select_items = columns.join(", ");
        let query = Parser::parse_sql_query(&format!("select {} from {}", select_items, table)).unwrap();

        Ok(Self {
            prefix: "t_".to_string(),
            ctes: vec![query],
            schema: Arc::new(schema.clone()),
            session_context: Arc::new(conn.session_context()?),
            conn,
        })
    }

    pub fn as_query(&self) -> Query {
        query_chain_to_cte(self.ctes.as_slice(), &self.prefix)
    }

    fn chain_query_str(&self, query: &str) -> Result<Arc<Self>> {
        let query_ast = Parser::parse_sql_query(query).map_err(|err| {
            VegaFusionError::internal(err.to_string())
        })?;
        self.chain_query(query_ast)
    }

    fn chain_query(&self, query: Query) -> Result<Arc<Self>> {
        let mut new_ctes = self.ctes.clone();
        new_ctes.push(query);

        let combined_query = query_chain_to_cte(new_ctes.as_slice(), &self.prefix);
        let query_str = combined_query.sql(&Dialect::datafusion()).expect("Failed to create query");
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

    fn sort(&self, columns: &[&str]) -> Result<Arc<Self>> {
        let mut query = Parser::parse_sql_query("select * from parent").unwrap();

        let order_by_expr: Vec<_> = columns.iter().map(|col| {
            OrderByExpr {
                expr: Expr::Identifier(Ident { value: col.to_string(), quote_style: None }),
                asc: None,
                nulls_first: None
            }
        }).collect();

        query.order_by = order_by_expr;
        self.chain_query(query)
    }

    async fn collect(&self) -> Result<VegaFusionTable> {
        self.conn.fetch_query(&self.as_query(), &self.schema).await
    }
}

fn query_chain_to_cte(queries: &[Query], prefix: &str) -> Query {
    // Rename the "parent" table in each query to refer to prior CTE stage
    let mut queries = Vec::from(queries);
    queries.iter_mut().enumerate().for_each(|(i, query)| {
        let parent_cte_name = format!("{}{}", prefix, i);
        rename_table_in_query(query, "parent", &parent_cte_name);
    });

    // Build vector of CTE AST nodes for all but the last query
    let cte_tables: Vec<_> = queries.iter().enumerate().map(|(i, query)| {
        let this_cte_name = format!("{}{}", prefix, i+1);
        Cte {
            alias: TableAlias { name: Ident { value: this_cte_name, quote_style: None }, columns: vec![] },
            query: query.clone(),
            from: None
        }
    }).collect();

    // The final query becomes the top-level query with CTEs attached to it
    let mut final_query = queries[queries.len() - 1].clone();
    final_query.with = if cte_tables.is_empty() {
        None
    } else {
        Some(With {
            recursive: false,
            cte_tables
        })
    };

    final_query
}

fn rename_table_in_query(query: &mut Query, old_name: &str, new_name: &str) {
    rename_tables_in_set_expr(&mut query.body, old_name, new_name)

}

fn rename_tables_in_set_expr(set_expr: &mut SetExpr, old_name: &str, new_name: &str) {
    match set_expr {
        SetExpr::Select(select) => {
            let from: &mut Vec<TableWithJoins> = &mut select.from;
            for twj in from.iter_mut() {
                rename_tables_in_table_factor(&mut twj.relation, old_name, new_name);
                for join in twj.joins.iter_mut() {
                    rename_tables_in_table_factor(&mut join.relation, old_name, new_name);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            rename_tables_in_set_expr(left, old_name, new_name);
            rename_tables_in_set_expr(right, old_name, new_name);
        }
        _ => {}
    }
}

fn rename_tables_in_table_factor(table_factor: &mut TableFactor, old_name: &str, new_name: &str) {
    if let TableFactor::Table { name, .. } = table_factor {
        if name.0.len() == 1 && &name.0[0].value == old_name {
            name.0[0].value = new_name.to_string();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use sqlgen::dialect::{Dialect, DialectDisplay};
    use sqlx::SqlitePool;
    use vegafusion_core::arrow::datatypes::{DataType, Field, Schema};
    use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
    use crate::connection::sqlite::SqLiteConnection;
    use crate::dataframe::SqlDataFrame;

    #[tokio::test]
    async fn try_it() {
        let schema = Schema::new(vec![
            Field::new("index", DataType::Int64, true),
            Field::new("symbol", DataType::Utf8, true),
            Field::new("date", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]);

        let pool = SqlitePool::connect(&"/media/jmmease/SSD2/rustDev/vega-fusion/vega-fusion/vegafusion-sql/tests/data/vega_datasets.db")
            .await
            .unwrap();

        let conn = SqLiteConnection::new(
            Arc::new(pool), "stock", &schema
        );

        let df = SqlDataFrame::try_new(Arc::new(conn), "stock").unwrap();
        println!("{:#?}", df.schema);

        // Transform query
        let df = df.chain_query_str("select date, symbol, price, price * 2 as dbl_price from parent order by date").unwrap();
        println!("{:#?}", df.schema);

        // Extract SQL in the sqlite dialect
        let s = df.as_query().sql(&Dialect::sqlite()).unwrap();
        println!("{}", s);

        let table = df.collect().await.unwrap();
        println!("{}", table.pretty_format(Some(10)).unwrap());
    }
}
