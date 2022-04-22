/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, VegaFusionError};
use crate::proto::gen::transforms::{Collect, SortOrder};
use crate::spec::transform::collect::CollectTransformSpec;
use crate::spec::values::SortOrderSpec;
use crate::transform::TransformDependencies;

impl Collect {
    pub fn try_new(transform: &CollectTransformSpec) -> Result<Self> {
        let sort = &transform.sort;
        let fields = sort.field.to_vec();
        let order = match &sort.order {
            None => {
                vec![SortOrderSpec::Ascending; fields.len()]
            }
            Some(order) => {
                let order = order.to_vec();
                if order.len() == fields.len() {
                    order
                } else {
                    return Err(VegaFusionError::specification(
                        "Length of field and order must match in collect transform",
                    ));
                }
            }
        };

        let order: Vec<_> = order
            .iter()
            .map(|order| match order {
                SortOrderSpec::Descending => SortOrder::Descending as i32,
                SortOrderSpec::Ascending => SortOrder::Ascending as i32,
            })
            .collect();

        Ok(Self { fields, order })
    }
}

impl TransformDependencies for Collect {}
