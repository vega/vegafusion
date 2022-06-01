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
            .with_context(|| format!("Failed to parse {} as timezone", local_tz))?;
        let default_input_tz = default_input_tz
            .clone()
            .unwrap_or_else(|| local_tz.to_string());
        let default_input_tz = chrono_tz::Tz::from_str(&default_input_tz)
            .ok()
            .with_context(|| format!("Failed to parse {} as timezone", default_input_tz))?;
        Ok(Self {
            local_tz,
            default_input_tz,
        })
    }
}
