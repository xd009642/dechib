use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnOption, DataType, Expr, Statement};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use tracing::warn;

pub type ColumnDescriptors = BTreeMap<String, ColumnDescriptor>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub datatype: DataType,
    pub not_null: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub foreign_key: Option<(String, String)>,
    pub default: Option<Expr>,
    // skipping check and create index as things I shalln't support (yet)
}

impl Default for ColumnDescriptor {
    fn default() -> Self {
        Self {
            datatype: DataType::Unspecified,
            not_null: false,
            unique: false,
            primary_key: false,
            foreign_key: None,
            default: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableOptions {
    pub name: String,
    pub columns: ColumnDescriptors,
}

#[derive(Clone, Debug)]
pub enum Command {
    CreateTable(CreateTableOptions),
}

impl TryFrom<&Statement> for Command {
    type Error = anyhow::Error;

    fn try_from(statement: &Statement) -> Result<Self, Self::Error> {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let mut descriptor = BTreeMap::new();
                for col in columns {
                    let entry = descriptor.entry(col.name.to_string()).or_insert_with(|| {
                        ColumnDescriptor {
                            datatype: col.data_type.clone(),
                            ..Default::default()
                        }
                    });

                    for opt in &col.options {
                        if opt.name.is_some() {
                            // Of course we want a database to do the wrong thing if it gets
                            // something unexpected :clown_face:
                            warn!("Unhandled named constraint: {:?}", opt.name);
                        }
                        match &opt.option {
                            ColumnOption::NotNull => {
                                entry.not_null = true;
                            }
                            ColumnOption::Default(e) => {
                                entry.default = Some(e.clone());
                            }
                            ColumnOption::Unique { is_primary, .. } => {
                                entry.primary_key = *is_primary;
                                entry.unique = true;
                            }
                            ColumnOption::ForeignKey { .. } => {
                                anyhow::bail!("FOREIGN KEY not yet supported")
                            }
                            ColumnOption::Check(_) => anyhow::bail!("CHECK not yet supported"),
                            ColumnOption::OnUpdate(_) => {
                                anyhow::bail!("ON UPDATE not yet supported")
                            }
                            ColumnOption::Generated { .. } => {
                                anyhow::bail!("GENERATED not yet supported")
                            }
                            ColumnOption::Null
                            | ColumnOption::DialectSpecific(_)
                            | ColumnOption::CharacterSet(_)
                            | ColumnOption::Comment(_)
                            | ColumnOption::Options(_) => {}
                        }
                    }
                }
                Ok(Command::CreateTable(CreateTableOptions {
                    name: name.to_string(),
                    columns: descriptor,
                }))
            }
            e => {
                anyhow::bail!("Unsupported Statement: {}", e);
            }
        }
    }
}
