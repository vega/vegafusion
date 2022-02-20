use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;
use tokio::net::{TcpListener, TcpStream};
use tonic::{transport::Server, Request, Response, Status, Code};
use vegafusion_core::error::VegaFusionError;
use vegafusion_core::proto::gen::services::{QueryRequest, QueryResult};
use vegafusion_core::proto::gen::services::vega_fusion_runtime_server::{
    VegaFusionRuntime as TonicVegaFusionRuntime,
    VegaFusionRuntimeServer as TonicVegaFusionRuntimeServer,
};

use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;
use tokio_tungstenite::tungstenite::Message;
use futures_util::stream::StreamExt;
use futures_util::SinkExt;


#[derive(Clone)]
pub struct VegaFusionRuntimeGrpc {
    pub runtime: TaskGraphRuntime
}

impl VegaFusionRuntimeGrpc {
    pub fn new(runtime: TaskGraphRuntime) -> VegaFusionRuntimeGrpc {
        VegaFusionRuntimeGrpc { runtime }
    }
}

#[tonic::async_trait]
impl TonicVegaFusionRuntime for VegaFusionRuntimeGrpc  {
    async fn task_graph_query(&self, request: Request<QueryRequest>) -> Result<Response<QueryResult>, Status> {
        println!("grpc request...");
        let result = self.runtime.query_request(request.into_inner()).await;
        match result {
            Ok(result) => {
                println!("  response");
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
    let address = "127.0.0.1:50051".to_string();

    // Get from command line
    let capacity: Option<usize> = Some(128);
    let memory_limit: Option<usize> = Some(2e9 as usize);
    let tg_runtime = TaskGraphRuntime::new(capacity, memory_limit);

    let grpc_address = Some(address.as_str());
    let websocket_address = Some("127.0.0.1:8087");

    tokio::try_join!(
        grpc_server(grpc_address, tg_runtime.clone()),
        websocket_server(websocket_address, tg_runtime.clone()),
    )?;

    Ok(())
}


async fn grpc_server(address: Option<&str>, runtime: TaskGraphRuntime)
    -> Result<(), Box<dyn std::error::Error>>
{
    if let Some(address) = address {
        let addr = address.parse()?;
        let server = TonicVegaFusionRuntimeServer::new(
            VegaFusionRuntimeGrpc::new(runtime)
        );
        Server::builder()
            .add_service(server)
            .serve(addr)
            .await?;
    } else {
        // Nothing to do
    }
    Ok(())
}

async fn websocket_server(address: Option<&str>, runtime: TaskGraphRuntime) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(address) = address {
        // Create the event loop and TCP listener we'll accept connections on.
        let try_socket = TcpListener::bind(&address).await;
        let listener = try_socket.expect("Failed to bind");
        println!("Listening on: {}", address);

        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(accept_websocket_connection(stream, runtime.clone()));
        }
    } else {
        // Nothing to do
    }

    Ok(())
}


async fn accept_websocket_connection(stream: TcpStream, runtime: TaskGraphRuntime) -> Result<(), VegaFusionError> {
    let addr = stream
        .peer_addr()
        .expect("connected streams should have a peer address");
    println!("Peer address: {}", addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");

    println!("New WebSocket connection: {}", addr);

    while let Some(msg) = ws_stream.next().await {
        let msg = msg.or_else(|_| Err(VegaFusionError::internal("websocket connection failed")))?;

        // println!("msg: {:?}", msg);
        if let Message::Binary(request_bytes) = msg {
            let response_bytes = runtime
                .query_request_bytes(request_bytes.as_slice())
                .await?;

            let response = Message::binary(response_bytes);
            ws_stream
                .send(response)
                .await
                .or_else(|_| Err(VegaFusionError::internal("websocket response failed")))?;
        } else {
            return Err(VegaFusionError::internal("expected binary message"));
        }
    }

    Ok(())
}
