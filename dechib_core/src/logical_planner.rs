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
use std::rc::Rc;

/// Logical plan operation
pub enum LogicalPlan {
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

pub struct Union {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
}

pub struct Intersection {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
}

pub struct Difference {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
}

pub struct Selection {
    data: Rc<LogicalPlan>,
    /// How am I storing predicates?
    predicate: (),
}

pub struct Projection {
    data: Rc<LogicalPlan>,
    columns: Vec<String>,
}

pub struct Join {
    left: Rc<LogicalPlan>,
    right: Rc<LogicalPlan>,
    // Left table right table expression
    on: Vec<((), ())>,
    join_type: (), // inner outer
}

pub struct DuplicateElimination {
    data: Rc<LogicalPlan>,
}

pub struct Aggregation {
    data: Rc<LogicalPlan>,
    aggr_expr: (),
}

pub struct Sorting {
    data: Rc<LogicalPlan>,
    /// How am I storing predicates?
    cmp: (), // a Cmp expression
    direction: (), // ascending descending
}

pub struct Rename {
    data: Rc<LogicalPlan>,
    renaming: Vec<(String, String)>,
}
