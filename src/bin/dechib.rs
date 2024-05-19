use dechib::{setup_logging, Instance};

fn main() {
    setup_logging();

    let instance = Instance::new();

    instance.execute("CREATE TABLE Persons (ID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255), PRIMARY KEY(ID));");

    instance.execute("INSERT INTO Persons (LastName, FirstName, Address, City) VALUES ('McKenna', 'Daniel', 'Never you mind', 'London');");
}
