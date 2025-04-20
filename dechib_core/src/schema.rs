use crate::types::*;
use std::collections::BTreeMap;

pub struct Schema {
    pub tables: BTreeMap<String, ColumnDescriptors>,
}
