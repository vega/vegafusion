/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
pub mod aggregate;
pub mod bin;
pub mod collect;
pub mod extent;
pub mod filter;
pub mod formula;
pub mod impute;
pub mod joinaggregate;
pub mod pipeline;
pub mod project;
pub mod stack;
pub mod timeunit;
pub mod utils;
pub mod window;

use crate::expression::compiler::config::CompilationConfig;
use datafusion::dataframe::DataFrame;

use std::sync::Arc;
use vegafusion_core::error::Result;

use async_trait::async_trait;
use vegafusion_core::proto::gen::transforms::transform::TransformKind;
use vegafusion_core::proto::gen::transforms::Transform;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::TransformDependencies;

#[async_trait]
pub trait TransformTrait: TransformDependencies {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)>;
}

pub fn to_transform_trait(tx: &TransformKind) -> &dyn TransformTrait {
    match tx {
        TransformKind::Filter(tx) => tx,
        TransformKind::Extent(tx) => tx,
        TransformKind::Formula(tx) => tx,
        TransformKind::Bin(tx) => tx,
        TransformKind::Aggregate(tx) => tx,
        TransformKind::Collect(tx) => tx,
        TransformKind::Timeunit(tx) => tx,
        TransformKind::Joinaggregate(tx) => tx,
        TransformKind::Window(tx) => tx,
        TransformKind::Project(tx) => tx,
        TransformKind::Stack(tx) => tx,
        TransformKind::Impute(tx) => tx,
    }
}

#[async_trait]
impl TransformTrait for Transform {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        to_transform_trait(self.transform_kind())
            .eval(dataframe, config)
            .await
    }
}
