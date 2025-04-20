use crate::types::*;
use std::collections::BTreeMap;

pub struct Schema {
    pub tables: BTreeMap<String, ColumnDescriptors>,
}

impl Schema {
    pub fn contains_table(&self, table: impl AsRef<str>) -> bool {
        self.tables.contains_key(table.as_ref())
    }

    pub fn column_exists(&self, table: impl AsRef<str>, column: impl AsRef<str>) -> bool {
        if let Some(col) = self.tables.get(table.as_ref()) {
            col.contains_key(column.as_ref())
        } else {
            false
        }
    }

    /// Given the column name from the WHERE predicate and a list of tables to consider resolve the
    /// column name. Returns a (table, column) tuple or an error if it couldn't be resolved.
    pub fn resolve_column_name(
        &self,
        column: impl AsRef<str>,
        tables: &[String],
    ) -> anyhow::Result<(String, String)> {
        if tables.is_empty() {
            anyhow::bail!("No tables to evaluate");
        } else if tables.len() == 1 {
            if self.column_exists(&tables[0], column.as_ref()) {
                Ok((tables[0].clone(), column.as_ref().to_string()))
            } else {
                // TODO We should remove a possible `{TABLE}.` as well probably.
                anyhow::bail!("Ref `{}.{}` does not exist", tables[0], column.as_ref());
            }
        } else {
            // Our more thorough logic.
            for table in tables {
                if let Some(column_name) = column.as_ref().strip_prefix(table) {
                    if self.column_exists(table, &column_name) {
                        return Ok((table.to_string(), column_name.to_string()));
                    } else {
                        anyhow::bail!("Ref `{}` does not exist", column.as_ref());
                    }
                }
            }
            anyhow::bail!("Couldn't resolve ref `{}` to a table", column.as_ref());
        }
    }
}
