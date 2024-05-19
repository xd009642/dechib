use rocksdb::{Options, DB};

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
        let path = "_dechib_db";
        let db = DB::open_default(path).expect("Failed to create storage");
        Self { db }
    }

    pub fn handle(&self) -> &DB {
        &self.db
    }

    pub fn handle_mut(&mut self) -> &mut DB {
        &mut self.db
    }

    pub fn create_table(&mut self, name: impl AsRef<str>) -> anyhow::Result<()> {
        // So each table should be a column family so operations that operate on different tables
        // can happen concurrently (my current understanding)
        self.db.create_cf(name, &Options::default())?;
        Ok(())
    }
}
