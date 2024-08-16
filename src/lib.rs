use crate::query_engine::QueryEngine;
use crate::storage_engine::StorageEngine;
use crate::types::*;
use std::{env, path::Path};
use tracing::{debug, instrument};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub mod query_engine;
pub mod storage_engine;
pub mod types;

pub struct Instance {
    storage: StorageEngine,
    query: QueryEngine,
}

impl Instance {
    pub fn new_with_path(path: impl AsRef<Path>) -> Self {
        Self {
            storage: StorageEngine::new_with_path(path),
            query: QueryEngine::default(),
        }
    }

    pub fn new() -> Self {
        Self {
            storage: StorageEngine::new(),
            query: QueryEngine::default(),
        }
    }

    #[instrument(skip_all)]
    pub fn execute(&mut self, query: &str) -> anyhow::Result<()> {
        let statements = self.query.process_sql(query)?;
        for statement in &statements {
            debug!("Running: {:?}", statement);
            match statement {
                Command::CreateTable(opts) => {
                    self.storage.create_table(opts)?;
                }
                Command::Insert(opts) => {
                    self.storage.insert_rows(opts)?;
                }
                Command::Select(_) => {
                    anyhow::bail!("Currently don't support SELECT queries");
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn storage(&self) -> &StorageEngine {
        &self.storage
    }
}

pub fn setup_logging() {
    let filter = match env::var("DECHIB_LOG") {
        Ok(s) => EnvFilter::new(s),
        Err(_) => EnvFilter::new("dechib=trace,desql=info"),
    };

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::DataType;
    use std::collections::BTreeMap;
    use tracing_test::traced_test;
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
    #[traced_test]
    fn create_table() {
        let handle = TableHandle::new();
        let mut engine = Instance::new_with_path(&handle.path);

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

        engine.execute("CREATE TABLE users (id INTEGER UNSIGNED NOT NULL UNIQUE PRIMARY KEY, name TEXT NOT NULL);").unwrap();

        let metadata = engine.storage.table_metadata("users").unwrap();

        assert_eq!(metadata, columns);

        std::mem::drop(engine);

        // Can we open it again and have it work?

        let _engine = StorageEngine::new_with_path(&handle.path);
    }
}
