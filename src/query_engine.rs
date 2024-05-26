use crate::types::*;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use tracing::debug;

#[derive(Copy, Clone, Debug, Default)]
pub struct QueryEngine;

impl QueryEngine {
    pub fn process_sql(&self, sql: &str) -> anyhow::Result<Vec<Command>> {
        let dialect = GenericDialect {};
        let parsed = Parser::parse_sql(&dialect, sql)?;
        debug!(ast=?parsed, "parsed sql query");
        let mut res = Vec::with_capacity(parsed.len());

        for statement in &parsed {
            res.push(Command::try_from(statement)?);
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

    #[test]
    fn duplicate_column_in_insert() {
        let engine = QueryEngine::default();
        let res = engine
            .process_sql("INSERT INTO Persons (FirstName, FirstName) VALUES ('Daniel', 'Daniel');");
        assert!(res.is_err(), "{:?} should be error", res);
    }
}
