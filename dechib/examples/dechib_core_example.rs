use dechib_core::{setup_logging, Instance};
use tracing::info;

fn main() -> anyhow::Result<()> {
    setup_logging();

    let mut instance = Instance::new();

    info!("Create persons table");
    instance.execute("CREATE TABLE Persons (ID INT AUTO_INCREMENT PRIMARY KEY, LastName varchar(255) NOT NULL UNIQUE, FirstName varchar(255) UNIQUE, Address varchar(255), City varchar(255));")?;

    info!("Insert data");
    instance.execute("INSERT INTO Persons (LastName, FirstName, Address, City) VALUES ('McKenna', 'Daniel', 'Never you mind', 'London');")?;

    info!("Create house table");
    instance.execute("CREATE TABLE House (ID INT AUTO_INCREMENT PRIMARY KEY, address varchar(255) NOT NULL, owner INT, FOREIGN KEY (owner) REFERENCES Persons(ID));")?;

    info!("Read the persons");
    instance.execute("SELECT * FROM Persons")?;

    Ok(())
}
