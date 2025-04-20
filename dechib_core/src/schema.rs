use crate::types::*;
use std::collections::BTreeMap;

pub struct Schema {
    pub tables: BTreeMap<String, ColumnDescriptors>,
}

impl Schema {
    pub fn contains_table(&self, table: impl AsRef<str>) -> bool {
        self.tables.contains_key(table.as_ref())
    }
}
