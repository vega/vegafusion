/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::proto::gen::transforms::{SortOrder, Stack, StackOffset};
use crate::spec::transform::stack::{StackOffsetSpec, StackTransformSpec};
use crate::spec::values::SortOrderSpec;
use crate::transform::TransformDependencies;

impl Stack {
    pub fn try_new(spec: &StackTransformSpec) -> Result<Self> {
        // Extract offset
        let offset = match spec.offset() {
            StackOffsetSpec::Zero => StackOffset::Zero,
            StackOffsetSpec::Center => StackOffset::Center,
            StackOffsetSpec::Normalize => StackOffset::Normalize,
        };

        // Extract sort fields
        let sort_fields = spec
            .sort
            .as_ref()
            .map(|compare| compare.field.to_vec())
            .unwrap_or_default();

        // Extract sort order
        let sort = spec
            .sort
            .as_ref()
            .and_then(|compare| compare.order.clone())
            .map(|order| {
                order
                    .to_vec()
                    .iter()
                    .map(|order| match order {
                        SortOrderSpec::Descending => SortOrder::Descending as i32,
                        SortOrderSpec::Ascending => SortOrder::Ascending as i32,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Extract groupby fields
        let groupby = spec
            .groupby
            .as_ref()
            .map(|fields| fields.iter().map(|field| field.field()).collect::<Vec<_>>())
            .unwrap_or_default();

        // Extract aliases
        let alias0 = spec
            .as_()
            .get(0)
            .cloned()
            .unwrap_or_else(|| "y0".to_string());
        let alias1 = spec
            .as_()
            .get(1)
            .cloned()
            .unwrap_or_else(|| "y1".to_string());

        Ok(Self {
            field: spec.field.field(),
            offset: offset as i32,
            sort,
            sort_fields,
            groupby,
            alias_0: Some(alias0),
            alias_1: Some(alias1),
        })
    }
}

impl TransformDependencies for Stack {}
