use crate::schema::Schema;
use sqlparser::ast::{DataType, Query};

// Column Ref like { table, name, index, datatype }

pub struct ColumnRef {
    table: String,
    field: String,
    data_type: DataType,
}

pub struct BoundPlan {}

/// From here
pub fn bind_query(value: &Query, schema: &Schema) -> anyhow::Result<BoundPlan> {
    todo!()
}
