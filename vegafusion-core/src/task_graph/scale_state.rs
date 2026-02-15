use crate::spec::scale::ScaleTypeSpec;
use datafusion_common::ScalarValue;
use std::collections::HashMap;
use vegafusion_common::arrow::array::ArrayRef;

#[derive(Debug, Clone)]
pub struct ScaleState {
    pub scale_type: ScaleTypeSpec,
    pub domain: ArrayRef,
    pub range: ArrayRef,
    pub options: HashMap<String, ScalarValue>,
}
