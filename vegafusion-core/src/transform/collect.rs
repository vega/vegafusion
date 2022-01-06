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
