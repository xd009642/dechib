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
use sqlparser::ast::{Query, Select, SetExpr};
use std::rc::Rc;

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

        todo!("Need to apply more things");
    }
}

fn select_to_logical_plan(select: &Select) -> anyhow::Result<LogicalPlan> {
    todo!()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableScan {
    table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Union {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intersection {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Difference {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Selection {
    data: Rc<LogicalPlan>,
    /// How am I storing predicates?
    predicate: (),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    data: Rc<LogicalPlan>,
    columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Join {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
    // Left table right table expression
    on: Vec<((), ())>,
    join_type: (), // inner outer
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicateElimination {
    data: Rc<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aggregation {
    data: Rc<LogicalPlan>,
    aggr_expr: (),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sorting {
    data: Rc<LogicalPlan>,
    /// How am I storing predicates?
    cmp: (), // a Cmp expression
    direction: (), // ascending descending
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rename {
    data: Rc<LogicalPlan>,
    renaming: Vec<(String, String)>,
}
