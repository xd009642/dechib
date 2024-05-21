use crate::query_engine::QueryEngine;
use crate::storage_engine::StorageEngine;
use std::env;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub mod query_engine;
pub mod storage_engine;
pub mod types;

pub struct Instance {
    storage: StorageEngine,
    query: QueryEngine,
}

impl Instance {
    pub fn new() -> Self {
        Self {
            storage: StorageEngine::new(),
            query: QueryEngine::default(),
        }
    }

    pub fn execute(&self, query: &str) -> anyhow::Result<()> {
        self.query.create_execution_plan(query)?;
        todo!();
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
