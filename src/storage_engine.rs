use crate::types::*;
use anyhow::Context;
use postcard::{from_bytes, to_allocvec};
use rocksdb::{Options, WriteBatch, DB};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use tracing::debug;

const TABLE_METADATA_KEY: &'static str = "__metadata__";

pub struct StorageEngine {
    db: DB,
    auto_incs: BTreeMap<Entry, AtomicUsize>,
}

fn generate_pk_name(record: &Record, metadata: &ColumnDescriptors) -> String {
    todo!()
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct Entry {
    table: String,
    column: String,
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
        Self {
            db,
            auto_incs: BTreeMap::new(),
        }
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

        for (column, props) in create_table
            .columns
            .iter()
            .filter(|(_, v)| v.auto_increment)
        {
            let initial = AtomicUsize::new(0);
            let entry = Entry {
                table: name.to_string(),
                column: column.to_string(),
            };
            self.auto_incs.insert(entry, initial);
        }

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
        if let Some(bad_column) = insert_op
            .columns
            .iter()
            .find(|x| !metadata.contains_key(x.as_str()))
        {
            anyhow::bail!("Column {} not present in table", bad_column);
        }

        let mut values_to_add = vec![];

        for (column, desc) in metadata.iter() {
            if desc.needs_value() {
                // Now find missing columns that we need!
                if !insert_op.columns.contains(column) {
                    anyhow::bail!("Required column {} is missing", column)
                }
            } else if !insert_op.columns.contains(column) && desc.should_generate() {
                values_to_add.push(column);
            }
        }
        debug!("Adding {:?} to the records", values_to_add);

        // handle must exist if we got metadata
        let mut transaction = WriteBatch::default();
        let handle = self.db.cf_handle(&insert_op.table).unwrap();

        for mut record in insert_op.records() {
            // validate record

            // Add things like missing default fields
            for column in &values_to_add {
                todo!()
            }

            let pk = generate_pk_name(&record, &metadata);

            // If valid insert
            let record = to_allocvec(&record)?;
            transaction.put_cf(&handle, &pk, &record);
        }
        self.db.write(transaction)?;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{self, DataType, Expr};
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

    fn default_fixture() -> CreateTableOptions {
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

        let expr = Expr::Value(ast::Value::SingleQuotedString("London".to_string()));

        columns.insert(
            "city".to_string(),
            ColumnDescriptor {
                datatype: DataType::Text,
                not_null: true,
                default: Some(expr),
                ..Default::default()
            },
        );

        CreateTableOptions {
            name: "users".to_string(),
            columns,
        }
    }

    #[test]
    fn create_table() {
        let handle = TableHandle::new();
        let mut engine = StorageEngine::new_with_path(&handle.path);

        let opt = default_fixture();

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

        let opt = default_fixture();

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

    #[test]
    fn invalid_insert_ops() {
        let handle = TableHandle::new();
        let mut engine = StorageEngine::new_with_path(&handle.path);

        let opt = default_fixture();

        engine.create_table(&opt).unwrap();

        let insert = InsertOptions {
            table: "doesnt_exist".to_string(),
            columns: vec!["name".to_string()],
            values: vec![vec![Value::Text("Daniel".to_string()).into()]],
        };
        // Table doesn't exist should fail
        assert!(engine.insert_rows(&insert).is_err());

        let insert = InsertOptions {
            table: "users".to_string(),
            columns: vec!["city".to_string()],
            values: vec![vec![Value::Text("London".to_string()).into()]],
        };

        // Missing name column should fail as it's not-null
        assert!(engine.insert_rows(&insert).is_err());

        let insert = InsertOptions {
            table: "users".to_string(),
            columns: vec!["toshi".to_string()],
            values: vec![vec![Value::Text("London".to_string()).into()]],
        };

        // Missing name column should fail as it's not-null
        assert!(engine.insert_rows(&insert).is_err());

        // TODO mismatched types, foreign key violations, setting columns that shouldn't be set?
    }
}
