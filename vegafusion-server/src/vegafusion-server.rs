use tonic::{transport::Server, Request, Response, Status, Code};
use vegafusion_core::proto::gen::services::{QueryRequest, QueryResult};
use vegafusion_core::proto::gen::services::vega_fusion_runtime_server::{
    VegaFusionRuntime as TonicVegaFusionRuntime,
    VegaFusionRuntimeServer as TonicVegaFusionRuntimeServer,
};

use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

#[derive(Clone)]
pub struct VegaFusionRuntimeGrpc {
    runtime: TaskGraphRuntime
}

impl VegaFusionRuntimeGrpc {
    pub fn new(capacity: Option<usize>, memory_limit: Option<usize>) -> VegaFusionRuntimeGrpc {
        VegaFusionRuntimeGrpc {
            runtime: TaskGraphRuntime::new(capacity, memory_limit)
        }
    }
}

#[tonic::async_trait]
impl TonicVegaFusionRuntime for VegaFusionRuntimeGrpc  {
    async fn task_graph_query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResult>, Status> {
        // println!("Request...");
        let result = self.runtime.query_request(request.into_inner()).await;
        match result {
            Ok(result) => {
                // println!("  Response");
                Ok(Response::new(result))
            }
            Err(err) => {
                Err(Status::unknown(err.to_string()))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    // Get from command line
    let capacity: Option<usize> = Some(128);
    let memory_limit: Option<usize> = Some(2e9 as usize);
    let server = VegaFusionRuntimeGrpc::new(capacity, memory_limit);
    let server = TonicVegaFusionRuntimeServer::new(server);

    let grpc_web = true;
    if grpc_web {
        let server = tonic_web::config()
            // .allow_origins(vec!["127.0.0.1"])
            .allow_all_origins()
            .enable(server);

        Server::builder()
            .accept_http1(true)
            .add_service(server)
            .serve(addr)
            .await?;
    } else {
        Server::builder()
            .add_service(server)
            .serve(addr)
            .await?;
    };

    Ok(())
}
