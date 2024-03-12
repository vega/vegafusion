use crate::planning::plan::PlannerConfig;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::mark::MarkSpec;
use vegafusion_common::error::Result;

pub fn strip_encodings(client_spec: &mut ChartSpec, config: &PlannerConfig) -> Result<()> {
    let mut visitor = StripEncodingsVisitor { config };
    client_spec.walk_mut(&mut visitor)?;
    Ok(())
}

#[derive(Debug)]
pub struct StripEncodingsVisitor<'a> {
    pub config: &'a PlannerConfig,
}

impl<'a> MutChartVisitor for StripEncodingsVisitor<'a> {
    fn visit_non_group_mark(&mut self, mark: &mut MarkSpec, _scope: &[u32]) -> Result<()> {
        let Some(encode) = &mut mark.encode else {
            return Ok(());
        };
        for (_, encodings) in encode.encodings.iter_mut() {
            if self.config.strip_description_encoding {
                encodings.channels.remove("description");
            }

            if self.config.strip_aria_encoding {
                encodings.channels.remove("ariaRoleDescription");
            }

            if self.config.strip_tooltip_encoding {
                encodings.channels.remove("tooltip");
            }
        }
        Ok(())
    }
}
