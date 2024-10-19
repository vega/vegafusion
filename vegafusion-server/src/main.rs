use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};
use vegafusion_core::error::{ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::errors::error::Errorkind;
use vegafusion_core::proto::gen::errors::{Error, TaskGraphValueError};
use vegafusion_core::proto::gen::services::vega_fusion_runtime_server::{
    VegaFusionRuntime as TonicVegaFusionRuntime,
    VegaFusionRuntimeServer as TonicVegaFusionRuntimeServer,
};
use vegafusion_core::proto::gen::services::{
    pre_transform_extract_result, pre_transform_spec_result, pre_transform_values_result,
    query_request, query_result, PreTransformExtractResult, PreTransformSpecResult,
    PreTransformValuesResult, QueryRequest, QueryResult,
};
use vegafusion_core::proto::gen::tasks::TaskGraphValueResponse;
use vegafusion_core::proto::gen::tasks::{
    ResponseTaskValue,
    TaskValue as ProtoTaskValue,
};
use vegafusion_core::runtime::{VegaFusionRuntimeTrait, decode_inline_datasets};
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_core::task_graph::graph::ScopedVariable;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;

use clap::Parser;
use regex::Regex;
use vegafusion_core::proto::gen::pretransform::{
    PreTransformExtractDataset, PreTransformExtractRequest, PreTransformExtractResponse,
    PreTransformSpecOpts, PreTransformSpecRequest, PreTransformSpecResponse,
    PreTransformValuesOpts, PreTransformValuesRequest, PreTransformValuesResponse,
};
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;

#[derive(Clone)]
pub struct VegaFusionRuntimeGrpc {
    pub runtime: VegaFusionRuntime,
}

impl VegaFusionRuntimeGrpc {
    pub fn new(runtime: VegaFusionRuntime) -> VegaFusionRuntimeGrpc {
        VegaFusionRuntimeGrpc { runtime }
    }

    async fn query_request_message(
        &self,
        request: QueryRequest,
    ) -> Result<QueryResult, VegaFusionError> {
        match request.request {
            Some(query_request::Request::TaskGraphValues(task_graph_values)) => {
                let task_graph = Arc::new(task_graph_values.task_graph.unwrap());
                let indices = &task_graph_values.indices;
                let inline_datasets =
                    decode_inline_datasets(task_graph_values.inline_datasets)?;

                match self
                    .runtime
                    .query_request(task_graph, indices.as_slice(), &inline_datasets)
                    .await
                {
                    Ok(response_values) => {
                        let response_msg = QueryResult {
                            response: Some(query_result::Response::TaskGraphValues(
                                TaskGraphValueResponse { 
                                    response_values: response_values.into_iter().map(|v| v.into()).collect::<Vec<_>>() 
                                },
                            )),
                        };
                        Ok(response_msg)
                    }
                    Err(e) => {
                        let response_msg = QueryResult {
                            response: Some(query_result::Response::Error(Error {
                                errorkind: Some(Errorkind::Error(TaskGraphValueError {
                                    msg: e.to_string(),
                                })),
                            })),
                        };
                        Ok(response_msg)
                    }
                }
            }
            _ => Err(VegaFusionError::internal(
                "Invalid VegaFusionRuntimeRequest request",
            )),
        }
    }

    async fn pre_transform_spec_request(
        &self,
        request: PreTransformSpecRequest,
    ) -> Result<PreTransformSpecResult, VegaFusionError> {
        // Handle default options
        let opts = request.opts.unwrap_or_else(|| PreTransformSpecOpts {
            row_limit: None,
            preserve_interactivity: true,
            keep_variables: vec![],
            local_tz: "UTC".to_string(),
            default_input_tz: None,
        });

        // Decode inline datasets to VegaFusionDatasets
        let inline_datasets = decode_inline_datasets(request.inline_datasets)?;

        // Parse spec
        let spec: ChartSpec = serde_json::from_str(&request.spec)?;

        // Apply pre-transform spec
        let (transformed_spec, warnings) = self
            .runtime
            .pre_transform_spec(&spec, &inline_datasets, &opts)
            .await?;

        // Build result
        let response = PreTransformSpecResult {
            result: Some(pre_transform_spec_result::Result::Response(
                PreTransformSpecResponse {
                    spec: serde_json::to_string(&transformed_spec)
                        .with_context(|| "Failed to convert chart spec to string")?,
                    warnings,
                },
            )),
        };

        Ok(response)
    }

    async fn pre_transform_extract_request(
        &self,
        request: PreTransformExtractRequest,
    ) -> Result<PreTransformExtractResult, VegaFusionError> {
        // Extract and deserialize inline datasets
        let inline_pretransform_datasets = request.inline_datasets;
        let inline_datasets =
            decode_inline_datasets(inline_pretransform_datasets)?;
        let opts = request.opts.unwrap();

        // Parse spec
        let spec_string = request.spec;
        let spec: ChartSpec = serde_json::from_str(&spec_string)?;
        let (spec, datasets, warnings) = self
            .runtime
            .pre_transform_extract(&spec, &inline_datasets, &opts)
            .await?;

        // Build Response
        let proto_datasets = datasets
            .into_iter()
            .map(|dataset| {
                Ok(PreTransformExtractDataset {
                    name: dataset.name,
                    scope: dataset.scope,
                    table: dataset.table.to_ipc_bytes()?,
                })
            })
            .collect::<Result<Vec<_>, VegaFusionError>>()?;

        let response = PreTransformExtractResponse {
            spec: serde_json::to_string(&spec)?,
            datasets: proto_datasets,
            warnings,
        };

        // Build result
        let result = PreTransformExtractResult {
            result: Some(pre_transform_extract_result::Result::Response(response)),
        };

        Ok(result)
    }

    pub async fn pre_transform_values_request(
        &self,
        request: PreTransformValuesRequest,
    ) -> Result<PreTransformValuesResult, VegaFusionError> {
        // Handle default options
        let opts = request
            .opts
            .clone()
            .unwrap_or_else(|| PreTransformValuesOpts {
                variables: vec![],
                row_limit: None,
                local_tz: "UTC".to_string(),
                default_input_tz: None,
            });

        // Extract and deserialize inline datasets
        let inline_datasets = decode_inline_datasets(request.inline_datasets)?;

        // Extract requested variables
        let variables: Vec<ScopedVariable> = request
            .opts
            .map(|opts| opts.variables)
            .unwrap_or_default()
            .into_iter()
            .map(|var| (var.variable.unwrap(), var.scope))
            .collect();

        // Parse spec
        let spec_string = request.spec;
        let spec: ChartSpec = serde_json::from_str(&spec_string)?;

        let (values, warnings) = self
            .runtime
            .pre_transform_values(&spec, &inline_datasets, &opts)
            .await?;

        let response_values: Vec<_> = values
            .iter()
            .zip(&variables)
            .map(|(value, var)| {
                let proto_value = ProtoTaskValue::try_from(value)?;
                Ok(ResponseTaskValue {
                    variable: Some(var.0.clone()),
                    scope: var.1.clone(),
                    value: Some(proto_value),
                })
            })
            .collect::<Result<Vec<_>, VegaFusionError>>()?;

        // Build result
        let result = PreTransformValuesResult {
            result: Some(pre_transform_values_result::Result::Response(
                PreTransformValuesResponse {
                    values: response_values,
                    warnings,
                },
            )),
        };

        Ok(result)
    }
}

#[tonic::async_trait]
impl TonicVegaFusionRuntime for VegaFusionRuntimeGrpc {
    async fn task_graph_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResult>, Status> {
        let result = self.query_request_message(request.into_inner()).await;
        match result {
            Ok(result) => Ok(Response::new(result)),
            Err(err) => Err(Status::unknown(err.to_string())),
        }
    }

    async fn pre_transform_spec(
        &self,
        request: Request<PreTransformSpecRequest>,
    ) -> Result<Response<PreTransformSpecResult>, Status> {
        let result = self.pre_transform_spec_request(request.into_inner()).await;
        match result {
            Ok(result) => Ok(Response::new(result)),
            Err(err) => Err(Status::unknown(err.to_string())),
        }
    }

    async fn pre_transform_extract(
        &self,
        request: Request<PreTransformExtractRequest>,
    ) -> Result<Response<PreTransformExtractResult>, Status> {
        let result = self
            .pre_transform_extract_request(request.into_inner())
            .await;
        match result {
            Ok(result) => Ok(Response::new(result)),
            Err(err) => Err(Status::unknown(err.to_string())),
        }
    }

    async fn pre_transform_values(
        &self,
        request: Request<PreTransformValuesRequest>,
    ) -> Result<Response<PreTransformValuesResult>, Status> {
        let result = self
            .pre_transform_values_request(request.into_inner())
            .await;
        match result {
            Ok(result) => Ok(Response::new(result)),
            Err(err) => Err(Status::unknown(err.to_string())),
        }
    }
}

/// VegaFusion Server
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Hostname
    #[clap(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port for gRPC server
    #[clap(long, default_value = "50051")]
    pub port: u32,

    /// Cache capacity
    #[clap(long, default_value = "64")]
    pub capacity: usize,

    /// Cache memory limit
    #[clap(long)]
    pub memory_limit: Option<String>,

    /// Include compatibility with gRPC-Web
    #[clap(long, num_args = 0)]
    pub web: bool,
}

#[tokio::main]
async fn main() -> Result<(), VegaFusionError> {
    let args = Args::parse();

    // Create addresse
    let grpc_address = format!("{}:{}", args.host, args.port);

    // Log Capacity limit
    println!("Cache capacity limit: {} entries", args.capacity);

    // Handle memory
    let memory_limit = if let Some(memory_limit) = &args.memory_limit {
        let memory_limit = parse_memory_string(memory_limit)?;
        println!("Cache memory limit: {memory_limit} bytes");
        Some(memory_limit)
    } else {
        println!("No cache memory limit");
        None
    };

    let tg_runtime = VegaFusionRuntime::new(
        Arc::new(DataFusionConnection::default()),
        Some(args.capacity),
        memory_limit,
    );

    grpc_server(grpc_address, tg_runtime.clone(), args.web)
        .await
        .expect("Failed to start grpc service");

    Ok(())
}

fn parse_memory_string(memory_limit: &str) -> Result<usize, VegaFusionError> {
    let pattern = Regex::new(r"(^\d+(\.\d+)?)(g|gb|gib|m|mb|mib|k|kb|kib|b)?$").unwrap();
    match pattern.captures(&memory_limit.to_lowercase()) {
        Some(captures) => {
            let amount: f64 = captures.get(1).unwrap().as_str().parse().unwrap();
            let suffix = captures.get(3).map(|c| c.as_str()).unwrap_or("b");
            let factor = match suffix {
                "b" => 1,
                "k" | "kb" => 1000,
                "kib" => 1024,
                "m" | "mb" => 1_000_000,
                "mib" => 1024 * 1024,
                "g" | "gb" => 1_000_000_000,
                "gib" => 1024 * 1024 * 1024,
                _ => panic!("Unreachable"),
            };
            let total = (amount * factor as f64) as usize;
            Ok(total)
        }
        None => Err(VegaFusionError::parse(format!(
            "Unable to parse memory limit: {memory_limit}"
        ))),
    }
}

async fn grpc_server(
    address: String,
    runtime: VegaFusionRuntime,
    web: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = address
        .parse()
        .ok()
        .with_context(|| format!("Failed to parse address: {address}"))?;
    let server = TonicVegaFusionRuntimeServer::new(VegaFusionRuntimeGrpc::new(runtime));

    if web {
        println!("Starting gRPC + gRPC-Web server on {address}");
        let server = tonic_web::enable(server);
        Server::builder()
            .accept_http1(true)
            .add_service(server)
            .serve(addr)
            .await?;
    } else {
        println!("Starting gRPC server on {address}");
        Server::builder().add_service(server).serve(addr).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::parse_memory_string;

    #[test]
    fn test_parse_memory_string() {
        fn check(memory_str: &str, expected: usize) {
            assert_eq!(parse_memory_string(memory_str).unwrap(), expected)
        }

        check("123", 123);
        check("123.2", 123);
        check("123b", 123);
        check("123.2b", 123);

        check("123kb", 123e3 as usize);
        check("123.2KB", 123.2e3 as usize);
        check("123Kib", 123 * 1024);
        check("123.2Kib", (123.2 * 1024.0) as usize);

        check("123mb", 123e6 as usize);
        check("123.2MB", 123.2e6 as usize);
        check("123Mib", 123 * 1024 * 1024);
        check("123.2Mib", (123.2 * 1024.0 * 1024.0) as usize);

        check("123gb", 123e9 as usize);
        check("123.2GB", 123.2e9 as usize);
        check("123Gib", 123 * 1024 * 1024 * 1024);
        check("123.2Gib", (123.2 * 1024.0 * 1024.0 * 1024.0) as usize);
    }
}
