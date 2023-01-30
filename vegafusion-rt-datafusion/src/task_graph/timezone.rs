use std::str::FromStr;
use vegafusion_core::error::Result;
use vegafusion_core::error::ResultWithContext;

#[derive(Copy, Clone, Debug)]
pub struct RuntimeTzConfig {
    pub local_tz: chrono_tz::Tz,
    pub default_input_tz: chrono_tz::Tz,
}

impl RuntimeTzConfig {
    pub fn try_new(local_tz: &str, default_input_tz: &Option<String>) -> Result<Self> {
        let local_tz = chrono_tz::Tz::from_str(local_tz)
            .ok()
            .with_context(|| format!("Failed to parse {local_tz} as timezone"))?;
        let default_input_tz = default_input_tz
            .clone()
            .unwrap_or_else(|| local_tz.to_string());
        let default_input_tz = chrono_tz::Tz::from_str(&default_input_tz)
            .ok()
            .with_context(|| format!("Failed to parse {default_input_tz} as timezone"))?;
        Ok(Self {
            local_tz,
            default_input_tz,
        })
    }
}
