use crate::storage_engine::StorageEngine;
use crate::types::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::debug;

#[derive(Copy, Clone, Debug, Default)]
pub struct QueryEngine;

impl QueryEngine {
    pub fn process_sql(&self, sql: &str, storage: &StorageEngine) -> anyhow::Result<Vec<Command>> {
        let dialect = GenericDialect {};
        let parsed = Parser::parse_sql(&dialect, sql)?;
        debug!(ast=?parsed, "parsed sql query");
        let mut res = Vec::with_capacity(parsed.len());

        for statement in &parsed {
            res.push(Command::parse_statement(statement, storage)?);
        }
        Ok(res)
    }

    pub fn create_execution_plan(&self, query: &str) -> anyhow::Result<()> {
        let dialect = GenericDialect {};
        let parsed = Parser::parse_sql(&dialect, query)?;
        debug!(ast=?parsed, "parsed sql query");
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn duplicate_column_in_insert() {
        let handle = TableHandle::new();
        let storage = StorageEngine::new_with_path(&handle.path);
        let engine = QueryEngine::default();
        let res = engine.process_sql(
            "INSERT INTO Persons (FirstName, FirstName) VALUES ('Daniel', 'Daniel');",
            &storage,
        );
        assert!(res.is_err(), "{:?} should be error", res);
    }
}
