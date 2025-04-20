//! Goals:
//! From reading [Query Planning and Optimisation](https://15445.courses.cs.cmu.edu/spring2024/notes/15-optimization1.pdf)
//! Selection optimisations:
//!
//! 1. Perform filters as early as possible (predicate pushdown)
//! 2. Reorder predicates to apply most selective first
//! 3. Breakup complex predicate and pushing down (split conjunctive predicates)
//!
//! Projection optimisations:
//!
//! 1. Projections as early as possible for smaller tuples (projection pushdown)
//! 2. Project out all attributes except the ones requested or required
//!
//! Query rewrite optimisations
//!
//! 1. Remove impossible or unnecessary predicates
//! 2. Re-write by de-correlating or flattening nested subqueries
//! 3. Decompose nested query and result into a temporary table
//! 4. Merging predicates
use crate::expressions::Expression;
use crate::parser_utils::*;
use crate::schema::Schema;
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, TableFactor};
use tracing::{debug, info, warn};

/// Logical plan operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    /// A table to scan through, this will be a leaf data source for queries ig
    TableScan(TableScan),
    /// Set union
    Union(Union),
    /// Set intersection
    Intersection(Intersection),
    /// Set difference
    Difference(Difference),
    /// Selecting rows based on a predicate (also referred to as Filter sometimes)
    Selection(Selection),
    /// Projection - retrieving specfic columns
    Projection(Projection),
    /// Joining two relations based on a common attribute - equivalent to cartesian product and
    /// selection
    Join(Join),
    /// Remove duplicate elements
    DuplicateElimination(DuplicateElimination),
    /// Limit the number of outputs produced
    Limit(Limit),
    /// Aggregate values
    Aggregation(Aggregation),
    /// Sort rows based on a comparison expression and direction
    Sorting(Sorting),
    /// Rename columns
    Rename(Rename),
    /// Noop TODO do I need this?
    Noop,
}

impl LogicalPlan {
    /// Create a new `LogicalPlan`. In future `Schema` may become a value so I can make it mutable
    /// and add in temporary tables/views which may be created.
    pub fn new(value: &Query, schema: &Schema) -> anyhow::Result<Self> {
        let mut plan = match value.body.as_ref() {
            SetExpr::Select(select) => select_to_logical_plan(select, schema)?,
            _ => anyhow::bail!("Unsupported body: {:?}", value.body),
        };

        // TODO Need to apply more things

        match (value.limit.as_ref(), value.offset.as_ref()) {
            (Some(limit), Some(offset)) => {
                let take = extract_usize(limit);
                let skip = extract_usize(&offset.value);
                plan = LogicalPlan::Limit(Limit {
                    skip,
                    take,
                    input: Box::new(plan),
                });
            }
            (Some(limit), None) => {
                let take = extract_usize(limit);

                plan = LogicalPlan::Limit(Limit {
                    skip: None,
                    take,
                    input: Box::new(plan),
                });
            }
            (None, Some(offset)) => {
                let skip = extract_usize(&offset.value);
                plan = LogicalPlan::Limit(Limit {
                    skip,
                    take: None,
                    input: Box::new(plan),
                });
            }
            _ => {}
        }

        Ok(plan)
    }
}

fn select_to_logical_plan(select: &Select, schema: &Schema) -> anyhow::Result<LogicalPlan> {
    let mut tables = Vec::with_capacity(select.from.len());
    for table in &select.from {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string();
                if schema.contains_table(&table_name) {
                    tables.push(Box::new(LogicalPlan::TableScan(TableScan { table_name })));
                } else {
                    anyhow::bail!("Table `{}` does not exist", name);
                }
            }
            _ => anyhow::bail!("Unsupported relation: {:?}", table.relation),
        }
        if !table.joins.is_empty() {
            anyhow::bail!("Joins currently unsupported");
        }
    }
    let table_count = tables.len();

    let mut root_node = match tables.len() {
        0 => anyhow::bail!("expressions with no tables not supported"),
        1 => tables.remove(0),
        _ => anyhow::bail!("expression with multiple tables not supported"),
    };

    // Handle joins we should end up with one logical plan after this (I think?)

    // Apply WHERE before projects. (Maybe we want all WHEREs after joins for now but splitting to
    // ones that don't need the join then doing ones with the join afterwards is probably smarter)

    if let Some(where_expr) = &select.selection {
        let expr = Expression::try_from(where_expr)?;
        root_node = Box::new(LogicalPlan::Selection(Selection {
            data: root_node,
            predicate: Box::new(expr),
        }));
    }

    info!("Plan before we start projections: {:?}", root_node);

    let mut projection = Projection {
        data: root_node,
        columns: vec![],
        select_all: false,
    };

    for proj in &select.projection {
        match proj {
            SelectItem::UnnamedExpr(expr) => {
                debug!(expr=?expr, "Check select expression");
                if let Expr::Identifier(i) = expr {
                    let name = i.to_string();
                    if table_count == 1 {
                        // Maybe we still need to support stripping the table name here
                        projection.columns.push(name);
                    } else {
                        if let LogicalPlan::TableScan(scan) = projection.data.as_ref() {
                            if name.starts_with(&scan.table_name) {
                                let col_name =
                                    name.strip_prefix(&scan.table_name).unwrap().to_string();
                                projection.columns.push(col_name);
                                break;
                            }
                        }
                    }
                } else {
                    anyhow::bail!("Can only retrieve identifiers from tables currently");
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {}
            SelectItem::Wildcard(_opt) => {
                panic!("This won't work we need a schema of some sort");
                projection.select_all = true;
            }
            SelectItem::QualifiedWildcard(_, _) => {
                anyhow::bail!("Qualified wildcards are not supported")
            }
        }
    }

    info!("End logical plan? {:?}", projection);

    // Just do a union for now and ignore the predicates
    if projection.select_all || !projection.columns.is_empty() {
        Ok(LogicalPlan::Projection(projection))
    } else {
        warn!("Query resulted in nothing to do... Is this right?");
        Ok(LogicalPlan::Noop)
    }
}

fn condition_to_plan(expr: &Expr, root_plan: LogicalPlan) -> LogicalPlan {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            unimplemented!()
        }
        Expr::IsNotFalse(expr) | Expr::IsTrue(expr) => {}
        Expr::IsFalse(expr) | Expr::IsNotFalse(expr) => {}
        Expr::IsNull(expr) => {}
        Expr::IsNotNull(expr) => {}
        Expr::InList {
            expr,
            list,
            negated,
        } => {}
        Expr::UnaryOp { op, expr } => {}
        _ => unimplemented!(),
    }
    todo!();
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableScan {
    table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Union {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intersection {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    data: Box<LogicalPlan>,
    /// How am I storing predicates?
    predicate: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    data: Box<LogicalPlan>,
    columns: Vec<String>,
    select_all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
    // Left table right table expression
    on: Vec<((), ())>,
    join_type: (), // inner outer
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateElimination {
    data: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregation {
    data: Box<LogicalPlan>,
    aggr_expr: (),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sorting {
    data: Box<LogicalPlan>,
    /// How am I storing predicates?
    columns: Vec<String>,
    direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    data: Box<LogicalPlan>,
    renaming: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    skip: Option<usize>,
    take: Option<usize>,
    input: Box<LogicalPlan>,
}
