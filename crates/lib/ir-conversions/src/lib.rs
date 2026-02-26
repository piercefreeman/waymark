use waymark_proto::ast as ir;

pub fn literal_from_json_value(value: &serde_json::Value) -> ir::Expr {
    match value {
        serde_json::Value::Bool(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::BoolValue(*value)),
            })),
            span: None,
        },
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(value)),
                    })),
                    span: None,
                }
            } else {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(
                            number.as_f64().unwrap_or(0.0),
                        )),
                    })),
                    span: None,
                }
            }
        }
        serde_json::Value::String(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::StringValue(value.clone())),
            })),
            span: None,
        },
        serde_json::Value::Array(items) => ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: items.iter().map(literal_from_json_value).collect(),
            })),
            span: None,
        },
        serde_json::Value::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, value)| ir::DictEntry {
                    key: Some(literal_from_json_value(&serde_json::Value::String(
                        key.clone(),
                    ))),
                    value: Some(literal_from_json_value(value)),
                })
                .collect();
            ir::Expr {
                kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
                span: None,
            }
        }
        serde_json::Value::Null => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IsNone(true)),
            })),
            span: None,
        },
    }
}
