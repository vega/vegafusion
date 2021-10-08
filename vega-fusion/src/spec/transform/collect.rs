use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::spec::transform::TransformSpecTrait;
use crate::spec::values::StringOrStringList;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectTransformSpec {
    pub sort: CollectSort,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectSort {
    pub field: StringOrStringList,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<SortOrderOrList>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Descending,
    Ascending,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortOrderOrList {
    SortOrder(SortOrder),
    SortOrderList(Vec<SortOrder>),
}

impl SortOrderOrList {
    pub fn to_vec(&self) -> Vec<SortOrder> {
        match self {
            SortOrderOrList::SortOrder(v) => vec![v.clone()],
            SortOrderOrList::SortOrderList(v) => v.clone(),
        }
    }
}

impl TransformSpecTrait for CollectTransformSpec {}
