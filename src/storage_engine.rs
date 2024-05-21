use crate::types::*;
use anyhow::Context;
use postcard::{from_bytes, to_allocvec};
use rocksdb::{Options, DB};
use std::path::Path;

const TABLE_METADATA_KEY: &'static str = "__metadata__";

pub struct StorageEngine {
    db: DB,
}

pub struct Entry {
    table: String,
    pk: String,
    column: String,
}

impl Entry {
    fn id(&self) -> String {
        format!("{}/{}/{}", self.table, self.pk, self.column)
    }
}

impl StorageEngine {
    pub fn new() -> Self {
        Self::new_with_path("_dechib_db")
    }

    pub fn new_with_path(path: impl AsRef<Path>) -> Self {
        let db = DB::open_default(path).expect("Failed to create storage");
        Self { db }
    }

    pub fn handle(&self) -> &DB {
        &self.db
    }

    pub fn handle_mut(&mut self) -> &mut DB {
        &mut self.db
    }

    pub fn create_table(
        &mut self,
        name: impl AsRef<str>,
        descriptor: &TableDescriptor,
    ) -> anyhow::Result<()> {
        // So each table should be a column family so operations that operate on different tables
        // can happen concurrently (my current understanding)
        self.db.create_cf(name.as_ref(), &Options::default())?;
        let handle = self.db.cf_handle(name.as_ref()).unwrap();
        self.db
            .put_cf(&handle, TABLE_METADATA_KEY, to_allocvec(descriptor)?)?;
        Ok(())
    }

    pub fn table_metadata(&self, name: impl AsRef<str>) -> anyhow::Result<TableDescriptor> {
        let handle = self
            .db
            .cf_handle(name.as_ref())
            .with_context(|| format!("No table {} exists", name.as_ref()))?;
        let bytes = self
            .db
            .get_pinned_cf(&handle, TABLE_METADATA_KEY)?
            .context("No metadata for table")?;
        let res = from_bytes(&bytes)?;
        Ok(res)
    }

    pub fn insert_row(&self, _name: impl AsRef<str>) -> anyhow::Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::DataType;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn create_table() {
        let path = format!("./target/{}", Uuid::new_v4());
        let mut engine = StorageEngine::new_with_path(&path);

        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnDescriptor {
                datatype: DataType::UnsignedInteger(None),
                not_null: true,
                unique: true,
                primary_key: true,
                ..Default::default()
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnDescriptor {
                datatype: DataType::Text,
                not_null: true,
                ..Default::default()
            },
        );

        engine.create_table("users", &columns).unwrap();

        let metadata = engine.table_metadata("users").unwrap();

        assert_eq!(metadata, columns);

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn metadata_error_on_nonexistant_table() {
        let path = format!("./target/{}", Uuid::new_v4());
        let mut engine = StorageEngine::new_with_path(&path);

        assert!(engine.table_metadata("users").is_err());

        let _ = std::fs::remove_dir_all(&path);
    }
}
