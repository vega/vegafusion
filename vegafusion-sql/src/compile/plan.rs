use datafusion_expr::logical_plan::{Filter, Projection};
use datafusion_expr::logical_plan::{LogicalPlan, TableScan};
use datafusion_expr::Expr;
use vegafusion_core::error::{Result, VegaFusionError};

use crate::ast::{
    ident::{Ident, ObjectName},
    query::{Query, SetExpr, TableAlias, TableFactor, TableWithJoins},
    select::{Select, SelectItem},
};
use crate::compile::expr::ToSqlExpr;

pub trait ToSqlQuery {
    fn to_sql(&self) -> Result<Query>;
}


impl ToSqlQuery for LogicalPlan {
    fn to_sql(&self) -> Result<Query> {
        Ok(match self {
            LogicalPlan::TableScan(scan) => scan.to_sql()?,
            LogicalPlan::Projection(proj) => proj.to_sql()?,
            LogicalPlan::Filter(filter) => filter.to_sql()?,
            LogicalPlan::Window(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Window cannot be converted to SQL",
                ))
            }
            LogicalPlan::Aggregate(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Aggregate cannot be converted to SQL",
                ))
            }
            LogicalPlan::Sort(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Sort cannot be converted to SQL",
                ))
            }
            LogicalPlan::Join(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Join cannot be converted to SQL",
                ))
            }
            LogicalPlan::CrossJoin(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::CrossJoin cannot be converted to SQL",
                ))
            }
            LogicalPlan::Repartition(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Repartition cannot be converted to SQL",
                ))
            }
            LogicalPlan::Union(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Union cannot be converted to SQL",
                ))
            }
            LogicalPlan::EmptyRelation(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::EmptyRelation cannot be converted to SQL",
                ))
            }
            LogicalPlan::Limit(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Limit cannot be converted to SQL",
                ))
            }
            LogicalPlan::CreateExternalTable(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::CreateExternalTable cannot be converted to SQL",
                ))
            }
            LogicalPlan::CreateMemoryTable(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::CreateMemoryTable cannot be converted to SQL",
                ))
            }
            LogicalPlan::CreateCatalogSchema(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::CreateCatalogSchema cannot be converted to SQL",
                ))
            }
            LogicalPlan::DropTable(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::DropTable cannot be converted to SQL",
                ))
            }
            LogicalPlan::Values(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Values cannot be converted to SQL",
                ))
            }
            LogicalPlan::Explain(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Explain cannot be converted to SQL",
                ))
            }
            LogicalPlan::Analyze(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Analyze cannot be converted to SQL",
                ))
            }
            LogicalPlan::Extension(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Extension cannot be converted to SQL",
                ))
            }
            LogicalPlan::Subquery(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Extension cannot be converted to SQL",
                ))
            }
            LogicalPlan::SubqueryAlias(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Extension cannot be converted to SQL",
                ))
            }
            LogicalPlan::Offset(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Extension cannot be converted to SQL",
                ))
            }
            LogicalPlan::CreateView(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Extension cannot be converted to SQL",
                ))
            }
            LogicalPlan::CreateCatalog(_) => {
                return Err(VegaFusionError::internal(
                    "LogicalPlan::Extension cannot be converted to SQL",
                ))
            }
        })
    }
}

impl ToSqlQuery for TableScan {
    fn to_sql(&self) -> Result<Query> {
        let table_name = self.table_name.clone();
        Ok(Query {
            with: None,
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![Ident {
                            value: table_name,
                            quote_style: None,
                        }]),
                        alias: None,
                        args: Default::default(),
                        with_hints: Default::default(),
                    },
                    joins: Default::default(),
                }],
                selection: None,
                group_by: Default::default(),
                having: None,
            })),
            order_by: Default::default(),
            limit: None,
            offset: None,
        })
    }
}


impl ToSqlQuery for Filter {
    fn to_sql(&self) -> Result<Query> {
        // Attempt to convert predicate to SQL
        let sql_expr = self.predicate.to_sql()?;
        let child = self.input.to_sql()?;

        // TODO: If child is already a selection without a filter (or groupby?), then override
        //       the filter to avoid unnecessary nesting

        // Treat child as subquery
        Ok(Query {
            with: None,
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(child),
                        alias: None,
                    },
                    joins: Default::default(),
                }],
                selection: Some(sql_expr),
                group_by: Default::default(),
                having: None,
            })),
            order_by: Default::default(),
            limit: None,
            offset: None,
        })
    }
}



impl ToSqlQuery for Projection {
    fn to_sql(&self) -> Result<Query> {
        // Compute projection
        let projection: Vec<_> = self
            .expr
            .iter()
            .map(|expr| {
                if let Expr::Alias(inner_expr, alias) = expr {
                    Ok(SelectItem::ExprWithAlias {
                        expr: inner_expr.to_sql()?,
                        alias: Ident {
                            value: alias.clone(),
                            quote_style: None
                        },
                    })
                } else {
                    Ok(SelectItem::UnnamedExpr(expr.to_sql()?))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let alias = self.alias.as_ref().map(|alias| TableAlias {
            name: Ident {
                value: alias.clone(),
                quote_style: None
            },
            columns: Default::default(),
        });

        let child = self.input.to_sql()?;

        // If child is already a selection with a wildcard projection, then override projection
        // to avoid unnecessary nesting
        if let SetExpr::Select(select) = &child.body {
            let select: &Select = select.as_ref();
            if select.projection == vec![SelectItem::Wildcard] {
                let body = SetExpr::Select(Box::new(Select {
                    projection,
                    ..select.clone()
                }));

                return Ok(Query { body, ..child });
            }
        }

        // Otherwise, treat child as subquery
        Ok(Query {
            with: None,
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                projection,
                from: vec![TableWithJoins {
                    relation: TableFactor::Derived {
                        lateral: false,
                        subquery: Box::new(child),
                        alias,
                    },
                    joins: Default::default(),
                }],
                selection: None,
                group_by: Default::default(),
                having: None,
            })),
            order_by: Default::default(),
            limit: None,
            offset: None,
        })
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use super::ToSqlQuery;
    use datafusion::prelude::SessionContext;
    
    use datafusion_expr::{col, lit};
    use crate::ast::display::DialectDisplay;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    #[test]
    fn example1() {
        let ctx = SessionContext::new();

        // Register empty table, named foo, with schema
        let schema = Schema::new(vec![
            Field::new("A", DataType::Float64, true),
            Field::new("BB", DataType::Utf8, true),
        ]);
        let foo_table = EmptyTable::new(Arc::new(schema));
        ctx.register_table("foo", Arc::new(foo_table)).unwrap();

        // Get table as a DataFrame
        let df = ctx.table("foo").unwrap();

        let df = df.filter(col("A").gt_eq(lit(2.5))).unwrap();

        let plan = df.to_logical_plan().unwrap();

        println!("plan\n{:#?}", plan);

        let sql_plan = plan.to_sql().unwrap();
        println!("sql_plan\n{:#?}", sql_plan);

        println!("{}", sql_plan.try_to_string(&Default::default()).unwrap());
    }
}
