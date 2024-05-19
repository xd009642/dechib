use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use tracing::debug;

#[derive(Copy, Clone, Debug, Default)]
pub struct QueryEngine {}

impl QueryEngine {
    pub fn create_execution_plan(&self, query: &str) -> anyhow::Result<()> {
        let dialect = GenericDialect {};
        let parsed = Parser::parse_sql(&dialect, query)?;
        debug!(ast=?parsed, "parsed sql query");
        todo!()
    }
}
