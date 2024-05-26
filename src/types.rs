use anyhow::Context;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{self, ColumnOption, DataType, Expr, Insert, SetExpr, Statement};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::rc::Rc;
use tracing::warn;

pub type ColumnDescriptors = BTreeMap<String, ColumnDescriptor>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Text(String),
    Boolean(bool),
    Number(BigDecimal),
    Bytes(Vec<u8>),
    Null,
}

impl Value {
    /// Checks partially if this value matches the descriptor, can't check for uniqueness or key
    /// constraints.
    pub fn matches(&self, descriptor: &ColumnDescriptor) -> bool {
        if matches!(self, Self::Null if descriptor.not_null) {
            false
        } else {
            // Need to match datatypes :yawn:
            true
        }
    }
}

impl TryFrom<ast::Value> for Value {
    type Error = anyhow::Error;

    fn try_from(val: ast::Value) -> Result<Self, Self::Error> {
        let v = match val {
            ast::Value::SingleQuotedString(s)
            | ast::Value::EscapedStringLiteral(s)
            | ast::Value::DoubleQuotedString(s)
            | ast::Value::RawStringLiteral(s)
            | ast::Value::NationalStringLiteral(s) => Value::Text(s),
            ast::Value::Boolean(b) => Value::Boolean(b),
            ast::Value::Null => Value::Null,
            ast::Value::Number(n, b) => {
                // I don't think I care about longs...
                Value::Number(n)
            }
            ast::Value::HexStringLiteral(hex) => {
                Value::Bytes(hex::decode(&hex).context("Invalid hex string")?)
            }
            ast::Value::SingleQuotedByteStringLiteral(s)
            | ast::Value::DoubleQuotedByteStringLiteral(s) => {
                // TODO is this right?
                Value::Bytes(s.into_bytes())
            }
            e => anyhow::bail!("Unsupported ast Value"),
        };
        Ok(v)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub columns: BTreeMap<String, Rc<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub datatype: DataType,
    pub not_null: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub foreign_key: Option<(String, String)>,
    pub default: Option<Expr>,
    // skipping check and create index as things I shalln't support (yet)
}

impl ColumnDescriptor {
    pub fn needs_value(&self) -> bool {
        self.not_null && !(self.primary_key || self.auto_increment || self.default.is_some())
    }

    pub fn should_generate(&self) -> bool {
        self.auto_increment || self.default.is_some() || self.not_null
    }

    pub fn value_matches_type(&self, value: &Value) -> bool {
        todo!()
    }
}

impl Default for ColumnDescriptor {
    fn default() -> Self {
        Self {
            datatype: DataType::Unspecified,
            not_null: false,
            auto_increment: false,
            unique: false,
            primary_key: false,
            foreign_key: None,
            default: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Command {
    CreateTable(CreateTableOptions),
    Insert(InsertOptions),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTableOptions {
    pub name: String,
    pub columns: ColumnDescriptors,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertOptions {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Rc<Value>>>,
}

impl InsertOptions {
    pub fn records(&self) -> impl Iterator<Item = Record> + '_ {
        self.values.iter().map(|row| Record {
            columns: self
                .columns
                .iter()
                .zip(row)
                .map(|(col, val)| (col.to_string(), val.clone()))
                .collect(),
        })
    }

    fn is_empty(&self) -> bool {
        self.columns.is_empty() || self.values.is_empty()
    }
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
            Statement::Insert(insert) => process_insert(insert),
            e => {
                anyhow::bail!("Unsupported Statement: {}", e);
            }
        }
    }
}

pub fn process_insert(insert: &Insert) -> anyhow::Result<Command> {
    let columns = insert.columns.iter().map(|x| x.to_string()).collect();
    let mut dup_check = HashSet::new();
    for col in &columns {
        if !dup_check.insert(col) {
            anyhow::bail!("Column '{}' is present multiple times in insert query", col);
        }
    }
    let mut values = vec![];
    if let Some(source) = &insert.source {
        match source.body.as_ref() {
            SetExpr::Values(v) => {
                for row in &v.rows {
                    let mut my_row = vec![];
                    for val in row {
                        match val {
                            Expr::Value(v) => {
                                my_row.push(Value::try_from(v.clone())?.into());
                            }
                            e => anyhow::bail!("Unhandled expression type: {}", e),
                        }
                    }
                    values.push(my_row);
                }
            }
            e => anyhow::bail!("Unhandled set expression: {}", e),
        }
    }

    Ok(Command::Insert(InsertOptions {
        table: insert.table_name.to_string(),
        columns,
        values,
    }))
}
