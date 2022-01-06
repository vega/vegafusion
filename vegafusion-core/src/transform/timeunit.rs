/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
use crate::error::Result;
use crate::proto::gen::tasks::Variable;
use crate::proto::gen::transforms::{TimeUnit, TimeUnitTimeZone, TimeUnitUnit};
use crate::spec::transform::timeunit::{
    TimeUnitTimeZoneSpec, TimeUnitTransformSpec, TimeUnitUnitSpec,
};
use crate::transform::TransformDependencies;

impl TimeUnit {
    pub fn try_new(transform: &TimeUnitTransformSpec) -> Result<Self> {
        let field = transform.field.clone();
        let units: Vec<_> = transform
            .units
            .iter()
            .flat_map(|units| units.iter().map(|unit| TimeUnitUnit::from(unit) as i32))
            .collect();
        let signal = transform.signal.clone();

        let alias_0 = transform.as_.as_ref().and_then(|v| v.get(0).cloned());
        let alias_1 = transform.as_.as_ref().and_then(|v| v.get(1).cloned());

        let timezone = match &transform.timezone {
            None => None,
            Some(timezone) => match timezone {
                TimeUnitTimeZoneSpec::Local => Some(TimeUnitTimeZone::Local as i32),
                TimeUnitTimeZoneSpec::Utc => Some(TimeUnitTimeZone::Utc as i32),
            },
        };

        Ok(Self {
            field,
            units,
            signal,
            alias_0,
            alias_1,
            timezone,
        })
    }
}

impl From<&TimeUnitUnitSpec> for TimeUnitUnit {
    fn from(unit: &TimeUnitUnitSpec) -> Self {
        match unit {
            TimeUnitUnitSpec::Year => Self::Year,
            TimeUnitUnitSpec::Quarter => Self::Quarter,
            TimeUnitUnitSpec::Month => Self::Month,
            TimeUnitUnitSpec::Date => Self::Date,
            TimeUnitUnitSpec::Week => Self::Week,
            TimeUnitUnitSpec::Day => Self::Day,
            TimeUnitUnitSpec::DayOfYear => Self::DayOfYear,
            TimeUnitUnitSpec::Hours => Self::Hours,
            TimeUnitUnitSpec::Minutes => Self::Minutes,
            TimeUnitUnitSpec::Seconds => Self::Seconds,
            TimeUnitUnitSpec::Milliseconds => Self::Milliseconds,
        }
    }
}

impl TransformDependencies for TimeUnit {
    fn output_vars(&self) -> Vec<Variable> {
        self.signal
            .iter()
            .map(|s| Variable::new_signal(s))
            .collect()
    }
}
