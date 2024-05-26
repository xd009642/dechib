use dechib::{setup_logging, Instance};

fn main() -> anyhow::Result<()> {
    setup_logging();

    let mut instance = Instance::new();

    instance.execute("CREATE TABLE Persons (ID INT AUTO_INCREMENT PRIMARY KEY, LastName varchar(255) NOT NULL UNIQUE, FirstName varchar(255) UNIQUE, Address varchar(255), City varchar(255));")?;

    instance.execute("INSERT INTO Persons (LastName, FirstName, Address, City) VALUES ('McKenna', 'Daniel', 'Never you mind', 'London');")?;

    Ok(())
}
