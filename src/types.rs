use serde::{Deserialize, Serialize};
use sqlparser::ast::DataType;
use std::collections::BTreeMap;

pub type TableDescriptor = BTreeMap<String, ColumnDescriptor>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub datatype: DataType,
    pub not_null: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub foreign_key: Option<(String, String)>,
    pub default: Option<Vec<u8>>,
    // skipping check and create index as things I shalln't support (yet)
}

impl Default for ColumnDescriptor {
    fn default() -> Self {
        Self {
            datatype: DataType::Unspecified,
            not_null: false,
            unique: false,
            primary_key: false,
            foreign_key: None,
            default: None,
        }
    }
}
