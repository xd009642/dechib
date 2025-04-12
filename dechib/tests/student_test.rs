use dechib_core::{setup_logging, Instance};
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
fn create_students_database() {
    let handle = TableHandle::new();
    let mut engine = Instance::new_with_path(&handle.path);
    let sql_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/students_test.sql");
    let queries = std::fs::read_to_string(sql_path).unwrap();

    engine.execute(&queries).unwrap();
}
