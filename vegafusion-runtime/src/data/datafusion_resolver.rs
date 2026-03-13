use std::sync::Arc;

use async_trait::async_trait;
use cfg_if::cfg_if;
use datafusion::prelude::{DataFrame, SessionContext};
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;
#[cfg(not(feature = "parquet"))]
use vegafusion_common::error::VegaFusionError;
use vegafusion_core::proto::gen::tasks::ResolverCapabilities;
use vegafusion_core::runtime::{ParsedUrl, ResolutionResult};

use super::plan_resolver::PlanResolver;

use super::tasks::{read_arrow, read_csv, read_json};
use super::util::DataFrameUtils;

#[cfg(feature = "parquet")]
use super::tasks::read_parquet;

/// Terminal `PlanResolver` that handles built-in URL formats and executes plans via DataFusion.
///
/// Handles http, https, file, and s3 schemes with csv, tsv, json, arrow, and parquet formats.
/// This is always the last resolver in a `ResolverPipeline`.
pub struct DataFusionResolver {
    pub(crate) ctx: Arc<SessionContext>,
}

impl DataFusionResolver {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
    }
}

#[async_trait]
impl PlanResolver for DataFusionResolver {
    fn name(&self) -> &str {
        "DataFusionResolver"
    }

    fn capabilities(&self) -> ResolverCapabilities {
        ResolverCapabilities::datafusion_defaults()
    }

    async fn scan_url(&self, parsed_url: &ParsedUrl) -> Result<Option<LogicalPlan>> {
        // Only handle schemes declared in our capabilities
        if !self
            .capabilities()
            .supported_schemes
            .contains(&parsed_url.scheme)
        {
            return Ok(None);
        }

        // Determine file type: format_type takes precedence over extension.
        // "json" format_type is treated as None (json is Vega-Lite's default,
        // shouldn't override extension detection).
        let file_type = match &parsed_url.format_type {
            Some(ft) if ft != "json" => Some(ft.as_str()),
            _ => None,
        };
        let ext = parsed_url.extension.as_deref();

        let url = &parsed_url.url;
        let ctx = self.ctx.clone();

        let df = if file_type == Some("csv") || (file_type.is_none() && ext == Some("csv")) {
            read_csv(url, &parsed_url.parse, ctx, false).await?
        } else if file_type == Some("tsv") || (file_type.is_none() && ext == Some("tsv")) {
            read_csv(url, &parsed_url.parse, ctx, true).await?
        } else if file_type == Some("json")
            || (file_type.is_none() && matches!(ext, Some("json") | None))
        {
            read_json(url, ctx).await?
        } else if file_type == Some("arrow")
            || (file_type.is_none() && matches!(ext, Some("arrow") | Some("feather")))
        {
            read_arrow(url, ctx).await?
        } else if file_type == Some("parquet") || (file_type.is_none() && ext == Some("parquet")) {
            cfg_if! {
                if #[cfg(feature = "parquet")] {
                    read_parquet(url, ctx).await?
                } else {
                    return Err(VegaFusionError::internal(
                        "Enable parquet support by enabling the `parquet` feature flag"
                    ))
                }
            }
        } else {
            // Unrecognized format — pass to next resolver
            return Ok(None);
        };

        Ok(Some(df.logical_plan().clone()))
    }

    async fn resolve_plan(&self, plan: LogicalPlan) -> Result<ResolutionResult> {
        let table = DataFrame::new(self.ctx.state(), plan)
            .collect_to_table()
            .await?;
        Ok(ResolutionResult::Table(table))
    }
}
