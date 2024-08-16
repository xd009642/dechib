use dechib::Instance;
use tempfile::{tempdir, TempDir};
use tracing_test::traced_test;

struct StudentDb {
    db: Instance,
    dir_handle: TempDir,
}

impl StudentDb {
    fn new() -> Self {
        let dir_handle = tempdir().unwrap();

        let db = Instance::new_with_path(dir_handle.path());
        Self { db, dir_handle }
    }

    fn init_default(&mut self) -> anyhow::Result<()> {
        self.create_student_table()?;
        self.create_professor_table()?;
        self.create_class_table()?;
        self.create_attends_table()
    }

    fn create_student_table(&mut self) -> anyhow::Result<()> {
        self.db.execute("create table student (id integer auto_increment, name varchar(20) not null, age integer not null, year integer not null, primary key (id) )")
    }

    fn create_professor_table(&mut self) -> anyhow::Result<()> {
        self.db.execute("create table professor (id integer auto_increment, name varchar(20) not null, primary key (id) )")
    }

    fn create_class_table(&mut self) -> anyhow::Result<()> {
        self.db.execute("create table class (id integer auto_increment, title varchar(50) not null, prof_id integer not null, primary key (id), foreign key (prof_id) references professor (id) )")
    }

    fn create_attends_table(&mut self) -> anyhow::Result<()> {
        self.db.execute("create table attends(student integer not null, lecture integer not null, grade integer not null, foreign key (student) references student(id), foreign key(lecture) references class(id))")
    }

    fn add_student(&mut self, name: &str, age: usize, year: usize) -> anyhow::Result<()> {
        self.db.execute(&format!(
            "INSERT INTO student (name, age, year) VALUES ('{name}', {age}, {year});"
        ))
    }

    fn add_professor(&mut self, name: &str) -> anyhow::Result<()> {
        self.db
            .execute(&format!("INSERT INTO professor (name ) VALUES ('{name}');"))
    }

    fn add_class(&mut self, title: &str, prof_id: usize) -> anyhow::Result<()> {
        self.db.execute(&format!(
            "INSERT INTO class (title, prof_id ) VALUES ('{title}', {prof_id});"
        ))
    }

    fn register_attendance(
        &mut self,
        student: usize,
        lecture: usize,
        grade: usize,
    ) -> anyhow::Result<()> {
        self.db.execute(&format!(
            "INSERT INTO attends (student, lecture, grade) VALUES ({student}, {lecture}, {grade});"
        ))
    }
}

/// The simplest of tests. We make sure we can create the student database and insert data.
/// In other tests we'll query this etc. But it's just nice to know we can make it first.
#[test]
#[traced_test]
fn student_simple() {
    let mut db = StudentDb::new();
    db.init_default().unwrap();
    db.add_student("Daniel McKenna", 31, 1).unwrap();
    // Read `Building Query Compilers` it's so useful!
    db.add_professor("Guido Moerkotte").unwrap();
    db.add_class("Building a query compiler", 1).unwrap();
    db.register_attendance(1, 1, 0).unwrap();
}

#[test]
#[traced_test]
fn invalid_foreign_keys() {
    let mut db = StudentDb::new();
    assert!(db.create_attends_table().is_err());
    assert!(db.create_class_table().is_err());

    db.create_professor_table().unwrap();
    db.create_class_table().unwrap();
    assert!(db.create_attends_table().is_err());
    db.create_student_table().unwrap();
    db.create_attends_table().unwrap();

    let res= db.db.execute("create table attends(student integer not null, lecture integer not null, grade integer not null, foreign key (student) references student(id), foreign key(lecture) references lecture(id))");
    assert!(res.is_err());
}
