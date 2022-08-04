use std::any::Any;
use std::fmt::format;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::SchemaRef;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::error::{Result, ResultWithContext};
use async_trait::async_trait;
use sqlgen::ast::{OrderByExpr, Query, TableAlias};
use sqlgen::ast::Expr;
use sqlgen::ast::Ident;
use sqlgen::ast::{Cte, With};
use sqlgen::parser::Parser;
use crate::connection::SqlDatabaseConnection;
use crate::dataframe::DataFrame;


#[derive(Clone)]
pub struct SqlDataFrame {
    prefix: String,
    schema: SchemaRef,
    ctes: Vec<Query>,
    conn: Arc<dyn SqlDatabaseConnection>
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
            conn,
        })
    }

    pub fn _next_cte_name(&self) -> String {
        format!("{}{}", self.prefix, self.ctes.len())
    }

    pub fn query(&self) -> Query {
        let cte_tables: Vec<_> = self.ctes[0..self.ctes.len()-1].iter().enumerate().map(|(i, query)| {
            let cte_name = format!("{}{}", self.prefix, i+1);
            Cte {
                alias: TableAlias { name: Ident { value: cte_name, quote_style: None }, columns: vec![] },
                query: query.clone(),
                from: None
            }
        }).collect();

        let mut final_query = self.ctes[self.ctes.len() - 1].clone();
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
}

#[async_trait]
impl DataFrame for SqlDataFrame {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn sort(&self, columns: &[&str]) -> Result<Arc<dyn DataFrame>> {
        let new_schema = self.schema.clone();
        let name = self._next_cte_name();
        let mut query = Parser::parse_sql_query(&format!("select * from {}", name)).unwrap();

        let order_by_expr: Vec<_> = columns.iter().map(|col| {
            OrderByExpr {
                expr: Expr::Identifier(Ident { value: col.to_string(), quote_style: None }),
                asc: None,
                nulls_first: None
            }
        }).collect();

        query.order_by = order_by_expr;
        let mut new_ctes = self.ctes.clone();
        new_ctes.push(query);

        Ok(Arc::new(SqlDataFrame {
            prefix: self.prefix.clone(),
            schema: new_schema,
            ctes: new_ctes,
            conn: self.conn.clone()
        }))
    }

    async fn collect(&self) -> Result<VegaFusionTable> {
        self.conn.fetch_query(&self.query(), &self.schema).await
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use sqlgen::dialect::DialectDisplay;
    use sqlx::SqlitePool;
    use vegafusion_core::arrow::datatypes::{DataType, Field, Schema};
    use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
    use crate::connection::sqlite::SqLiteConnection;
    use crate::dataframe::sql::SqlDataFrame;
    use crate::dataframe::DataFrame;

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

        let df = df.sort(vec!["index"].as_slice()).unwrap();

        let df = df.sort(vec!["symbol", "price"].as_slice()).unwrap();

        // Now downcase back to SqlDataFrame
        let df = df.as_any().downcast_ref::<SqlDataFrame>().unwrap();


        let s = df.query().sql(&Default::default()).unwrap();
        println!("{}", s);

        let table = df.collect().await.unwrap();
        println!("{}", table.pretty_format(Some(10)).unwrap());
    }
}