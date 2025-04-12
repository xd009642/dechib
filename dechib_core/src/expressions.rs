use crate::types::Value;
use sqlparser::ast::Expr;

pub enum Expression {}

impl TryFrom<Expr> for Expression {
    type Error = anyhow::Error;

    fn try_from(expr: Expr) -> Result<Self, Self::Error> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                unimplemented!()
            }
            Expr::IsNotFalse(expr) | Expr::IsTrue(expr) => {}
            Expr::IsFalse(expr) | Expr::IsNotFalse(expr) => {}
            Expr::IsNull(expr) => {}
            Expr::IsNotNull(expr) => {}
            Expr::InList {
                expr,
                list,
                negated,
            } => {}
            Expr::UnaryOp { op, expr } => {}
            _ => anyhow::bail!("Unsupported expression: {:?}", expr),
        }
        todo!()
    }
}
