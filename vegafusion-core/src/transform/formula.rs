use crate::error::Result;
use crate::expression::parser::parse;
use crate::proto::gen::transforms::Formula;
use crate::spec::transform::formula::FormulaTransformSpec;

impl Formula {
    pub fn try_new(spec: &FormulaTransformSpec) -> Result<Self> {
        let expr = parse(&spec.expr)?;
        Ok(Self {
            expr: Some(expr),
            r#as: spec.as_.clone(),
        })
    }
}
