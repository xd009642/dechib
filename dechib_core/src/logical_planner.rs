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
use serde::{Deserialize, Serialize};
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, TableFactor};

/// Logical plan operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Aggregate values
    Aggregation(Aggregation),
    /// Sort rows based on a comparison expression and direction
    Sorting(Sorting),
    /// Rename columns
    Rename(Rename),
}

impl TryFrom<&Query> for LogicalPlan {
    type Error = anyhow::Error;

    fn try_from(value: &Query) -> Result<Self, Self::Error> {
        let plan = match value.body.as_ref() {
            SetExpr::Select(select) => select_to_logical_plan(select)?,
            _ => anyhow::bail!("Unsupported body: {:?}", value.body),
        };

        // TODO Need to apply more things

        Ok(plan)
    }
}

fn select_to_logical_plan(select: &Select) -> anyhow::Result<LogicalPlan> {
    let mut tables = Vec::with_capacity(select.from.len());
    for table in &select.from {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                tables.push(Box::new(LogicalPlan::TableScan(TableScan {
                    table_name: name.to_string(),
                })));
            }
            _ => anyhow::bail!("Unsupported relation: {:?}", table.relation),
        }
        if !table.joins.is_empty() {
            anyhow::bail!("Joins currently unsupported");
        }
    }

    let mut projections = tables
        .iter()
        .map(|x| Projection {
            data: x.clone(),
            columns: vec![],
            select_all: false,
        })
        .collect::<Vec<_>>();

    for proj in &select.projection {
        match proj {
            SelectItem::UnnamedExpr(expr) => {
                if tables.len() == 1 {
                    if let Expr::Identifier(i) = expr {
                        projections[0].columns.push(i.to_string());
                    } else {
                        anyhow::bail!("Can only retrieve identifiers from tables currently");
                    }
                } else {
                    // I expect I now need to split the table name off
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {}
            SelectItem::Wildcard(_opt) => {
                for proj in projections.iter_mut() {
                    proj.select_all = true;
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                anyhow::bail!("Qualified wildcards are not supported")
            }
        }
    }

    // Just do a union for now and ignore the predicates
    if projections.is_empty() {
        anyhow::bail!("Projections should not be empty");
    } else if projections.len() == 1 {
        Ok(LogicalPlan::Projection(projections.remove(0)))
    } else {
        let mut union = LogicalPlan::Union(Union {
            left: Box::new(LogicalPlan::Projection(projections.remove(0))),
            right: Box::new(LogicalPlan::Projection(projections.remove(0))),
        });

        for proj in projections.drain(..) {
            let temp_union = LogicalPlan::Union(Union {
                left: Box::new(LogicalPlan::Projection(proj)),
                right: Box::new(union),
            });
            union = temp_union;
        }
        Ok(union)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableScan {
    table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Union {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intersection {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    data: Box<LogicalPlan>,
    /// How am I storing predicates?
    predicate: (),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    data: Box<LogicalPlan>,
    columns: Vec<String>,
    select_all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Join {
    left: Box<LogicalPlan>,
    right: Box<LogicalPlan>,
    // Left table right table expression
    on: Vec<((), ())>,
    join_type: (), // inner outer
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateElimination {
    data: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aggregation {
    data: Box<LogicalPlan>,
    aggr_expr: (),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sorting {
    data: Box<LogicalPlan>,
    /// How am I storing predicates?
    cmp: (), // a Cmp expression
    direction: (), // ascending descending
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rename {
    data: Box<LogicalPlan>,
    renaming: Vec<(String, String)>,
}
