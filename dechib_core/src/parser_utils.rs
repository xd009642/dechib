use crate::types::Value;
use anyhow::Context;
use bigdecimal::{BigDecimal, ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{self, Expr};
use std::rc::Rc;

/// If a expression is a constant numbe extract it as a usize otherwise None.
pub fn extract_usize(expr: &Expr) -> Option<usize> {
    if let Expr::Value(val) = expr {
        let val = Value::try_from(val.clone()).ok()?;
        if let Value::Number(num) = val {
            return num.to_usize();
        }
    }
    None
}
