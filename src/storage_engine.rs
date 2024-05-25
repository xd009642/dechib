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
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = match DB::list_cf(&opts, path.as_ref()) {
            Ok(cf) => DB::open_cf(&opts, path, &cf).expect("Failed to load storage"),
            Err(_) => DB::open(&opts, path).expect("Failed to create storage"),
        };
        Self { db }
    }

    pub fn handle(&self) -> &DB {
        &self.db
    }

    pub fn handle_mut(&mut self) -> &mut DB {
        &mut self.db
    }

    pub fn create_table(&mut self, create_table: &CreateTableOptions) -> anyhow::Result<()> {
        // So each table should be a column family so operations that operate on different tables
        // can happen concurrently (my current understanding)
        let name = create_table.name.as_ref();
        self.db.create_cf(name, &Options::default())?;
        let handle = self.db.cf_handle(name).unwrap();
        self.db.put_cf(
            &handle,
            TABLE_METADATA_KEY,
            to_allocvec(&create_table.columns)?,
        )?;
        Ok(())
    }

    pub fn table_metadata(&self, name: impl AsRef<str>) -> anyhow::Result<ColumnDescriptors> {
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

    pub fn insert_rows(&mut self, insert_op: &InsertOptions) -> anyhow::Result<()> {
        // We should validate our metadata against our column data types!
        let metadata = self.table_metadata(&insert_op.table)?;

        // First lets just go over and make sure column names match etc

        // handle must exist if we got metadata
        let handle = self.db.cf_handle(&insert_op.table).unwrap();

        for record in insert_op.records() {
            // validate record

            // If valid insert
            let record = to_allocvec(&record)?;
            //handle.put_cf_opt(&handle,
            // If there's an invalid one should all inserts fail?
        }
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::DataType;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    struct TableHandle {
        path: String,
    }

    impl TableHandle {
        fn new() -> Self {
            Self {
                path: format!("./target/{}", Uuid::new_v4()),
            }
        }
    }

    impl Drop for TableHandle {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn create_table() {
        let handle = TableHandle::new();
        let mut engine = StorageEngine::new_with_path(&handle.path);

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

        let opt = CreateTableOptions {
            name: "users".to_string(),
            columns,
        };

        engine.create_table(&opt).unwrap();

        let metadata = engine.table_metadata("users").unwrap();

        assert_eq!(metadata, opt.columns);

        std::mem::drop(engine);

        // Can we open it again and have it work?

        let _engine = StorageEngine::new_with_path(&handle.path);
    }

    #[test]
    fn error_if_table_already_exists() {
        let handle = TableHandle::new();
        let mut engine = StorageEngine::new_with_path(&handle.path);
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

        let opt = CreateTableOptions {
            name: "users".to_string(),
            columns,
        };

        engine.create_table(&opt).unwrap();
        assert!(engine.create_table(&opt).is_err());
    }

    #[test]
    fn metadata_error_on_nonexistant_table() {
        let handle = TableHandle::new();
        let mut engine = StorageEngine::new_with_path(&handle.path);

        let path = format!("./target/{}", Uuid::new_v4());
        let mut engine = StorageEngine::new_with_path(&path);

        assert!(engine.table_metadata("users").is_err());
    }
}
