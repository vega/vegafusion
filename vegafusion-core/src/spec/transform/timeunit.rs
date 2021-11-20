use crate::spec::transform::TransformSpecTrait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeUnitTransformSpec {
    pub field: String, // TODO: support field object

    #[serde(skip_serializing_if = "Option::is_none")]
    pub units: Option<Vec<TimeUnitUnitSpec>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timezone: Option<TimeUnitTimeZoneSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub extent: Option<(String, String)>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxbins: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,

    #[serde(rename = "as", skip_serializing_if = "Option::is_none")]
    pub as_: Option<Vec<String>>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnitTimeZoneSpec {
    Local,
    Utc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnitUnitSpec {
    Year,
    Quarter,
    Month,
    Date,
    Week,
    Day,
    DayOfYear,
    Hours,
    Minutes,
    Seconds,
    Milliseconds,
}

impl TransformSpecTrait for TimeUnitTransformSpec {
    fn supported(&self) -> bool {
        !(self.units.is_none()
            || self.step.is_some()
            || self.extent.is_some()
            || self.maxbins.is_some()
            || self.signal.is_some())
    }

    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }
}
