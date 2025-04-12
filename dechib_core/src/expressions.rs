use crate::types::Value;
use sqlparser::ast::{self, Expr};

/// This type might end up looking a bit like the sqlparser expression type, but it'll have a lot
/// less stuff in it
pub enum Expression {
    Literal(Value),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    StringConcat,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Spaceship,
    Eq,
    NotEq,
    And,
    Or,
    Xor,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
}

impl TryFrom<&ast::BinaryOperator> for BinaryOperator {
    type Error = anyhow::Error;

    fn try_from(op: &ast::BinaryOperator) -> Result<Self, Self::Error> {
        let res = match op {
            ast::BinaryOperator::Plus => Self::Plus,
            ast::BinaryOperator::Minus => Self::Minus,
            ast::BinaryOperator::Multiply => Self::Multiply,
            ast::BinaryOperator::Divide => Self::Divide,
            ast::BinaryOperator::Modulo => Self::Modulo,
            ast::BinaryOperator::StringConcat => Self::StringConcat,
            ast::BinaryOperator::Gt => Self::Gt,
            ast::BinaryOperator::Lt => Self::Lt,
            ast::BinaryOperator::GtEq => Self::GtEq,
            ast::BinaryOperator::LtEq => Self::LtEq,
            ast::BinaryOperator::Spaceship => Self::Spaceship,
            ast::BinaryOperator::Eq => Self::Eq,
            ast::BinaryOperator::NotEq => Self::NotEq,
            ast::BinaryOperator::And => Self::And,
            ast::BinaryOperator::Or => Self::Or,
            ast::BinaryOperator::Xor => Self::Xor,
            ast::BinaryOperator::BitwiseOr => Self::BitwiseOr,
            ast::BinaryOperator::BitwiseAnd => Self::BitwiseAnd,
            ast::BinaryOperator::BitwiseXor => Self::BitwiseXor,
            other => anyhow::bail!("Unsupported operator: {}", other),
        };
        Ok(res)
    }
}

impl TryFrom<&Expr> for Expression {
    type Error = anyhow::Error;

    fn try_from(expr: &Expr) -> Result<Self, Self::Error> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left = Box::new(Expression::try_from(left.as_ref())?);
                let right = Box::new(Expression::try_from(right.as_ref())?);
                let op = BinaryOperator::try_from(op)?;
                Ok(Expression::BinaryOp { left, op, right })
            }
            Expr::IsNotTrue(expr) => {
                let left = Box::new(Expression::try_from(expr.as_ref())?);
                Ok(Expression::BinaryOp {
                    left,
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expression::Literal(Value::Boolean(true))),
                })
            }
            Expr::IsTrue(expr) => {
                let left = Box::new(Expression::try_from(expr.as_ref())?);
                Ok(Expression::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(Value::Boolean(true))),
                })
            }
            Expr::IsFalse(expr) => {
                let left = Box::new(Expression::try_from(expr.as_ref())?);
                Ok(Expression::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(Value::Boolean(false))),
                })
            }
            Expr::IsNotFalse(expr) => {
                let left = Box::new(Expression::try_from(expr.as_ref())?);
                Ok(Expression::BinaryOp {
                    left,
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expression::Literal(Value::Boolean(false))),
                })
            }
            Expr::IsNull(expr) => {
                let left = Box::new(Expression::try_from(expr.as_ref())?);
                Ok(Expression::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right: Box::new(Expression::Literal(Value::Null)),
                })
            }
            Expr::IsNotNull(expr) => {
                let left = Box::new(Expression::try_from(expr.as_ref())?);
                Ok(Expression::BinaryOp {
                    left,
                    op: BinaryOperator::NotEq,
                    right: Box::new(Expression::Literal(Value::Null)),
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                unimplemented!()
            }
            Expr::UnaryOp { op, expr } => {
                unimplemented!()
            }
            _ => anyhow::bail!("Unsupported expression: {:?}", expr),
        }
    }
}

impl Expression {
    // Evaluating an expression will need rows to operate on - maybe table info :thinking:
    pub fn evaluate(&self, args: &[Value]) -> Value {
        todo!()
    }

    fn visit(&self) {}
}
