use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::exit;
use std::str::FromStr;
use tokio::net::{TcpListener, TcpStream};
use tonic::{transport::Server, Code, Request, Response, Status};
use vegafusion_core::error::{ResultWithContext, ToExternalError, VegaFusionError};
use vegafusion_core::proto::gen::services::vega_fusion_runtime_server::{
    VegaFusionRuntime as TonicVegaFusionRuntime,
    VegaFusionRuntimeServer as TonicVegaFusionRuntimeServer,
};
use vegafusion_core::proto::gen::services::{QueryRequest, QueryResult};

use futures_util::stream::StreamExt;
use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

use clap::Parser;
use regex::{Captures, Regex};

#[derive(Clone)]
pub struct VegaFusionRuntimeGrpc {
    pub runtime: TaskGraphRuntime,
}

impl VegaFusionRuntimeGrpc {
    pub fn new(runtime: TaskGraphRuntime) -> VegaFusionRuntimeGrpc {
        VegaFusionRuntimeGrpc { runtime }
    }
}

#[tonic::async_trait]
impl TonicVegaFusionRuntime for VegaFusionRuntimeGrpc {
    async fn task_graph_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResult>, Status> {
        println!("grpc request...");
        let result = self.runtime.query_request(request.into_inner()).await;
        match result {
            Ok(result) => {
                println!("  response");
                Ok(Response::new(result))
            }
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
    #[clap(long)]
    pub grpc_port: Option<u32>,

    /// Port for Websocket server
    #[clap(long)]
    pub websocket_port: Option<u32>,

    /// Cache capacity
    #[clap(long, default_value="64")]
    pub capacity: usize,

    /// Cache memory limit
    #[clap(long)]
    pub memory_limit: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), VegaFusionError> {
    let args = Args::parse();
    if args.grpc_port.is_none() && args.websocket_port.is_none() {
        println!(
            "Error: At least one of --grpc-port or --websocket-port must be provided"
        );
        exit(1);
    }

    // Create gRPC and Websocket addresses
    let grpc_address = args.grpc_port.map(|p| format!("{}:{}", args.host, p));
    let websocket_address = args.websocket_port.map(|p| format!("{}:{}", args.host, p));

    // Log Capacity limit
    println!("Cache capacity limit: {} entries", args.capacity);

    // Handle memory
    let memory_limit = if let Some(memory_limit) = &args.memory_limit {
        let memory_limit = parse_memory_string(memory_limit)?;
        println!("Cache memory limit: {} bytes", memory_limit);
        Some(memory_limit)
    } else {
        println!("No cache memory limit");
        None
    };

    let tg_runtime = TaskGraphRuntime::new(
        Some(args.capacity), memory_limit
    );

    tokio::try_join!(
        grpc_server(grpc_address, tg_runtime.clone()),
        websocket_server(websocket_address, tg_runtime.clone()),
    ).expect("Server error");

    Ok(())
}

fn parse_memory_string(memory_limit: &str) -> Result<usize, VegaFusionError> {
    let pattern = Regex::new(
        r"(^\d+(\.\d+)?)(g|gb|gib|m|mb|mib|k|kb|kib|b)?$"
    ).unwrap();
    match pattern.captures(&memory_limit.to_lowercase()) {
        Some(captures) => {
            let amount: f64 = captures.get(1).unwrap().as_str().parse().unwrap();
            let suffix = captures.get(3).map(|c| c.as_str()).unwrap_or("b");
            let factor = match suffix {
                "b" => 1,
                "k" | "kb" => 1000,
                "kib" => 1024,
                "m" | "mb" => 1_000_000,
                "mib" => 1024*1024,
                "g" | "gb" => 1_000_000_000,
                "gib" => 1024*1024*1024,
                _ => panic!("Unreachable")
            };
            let total = (amount * factor as f64) as usize;
            Ok(total)
        }
        None => {
            Err(VegaFusionError::parse(
                format!("Unable to parse memory limit: {}", memory_limit))
            )
        }
    }
}

async fn grpc_server(
    address: Option<String>,
    runtime: TaskGraphRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(address) = address {
        let addr = address.parse().ok().with_context(
            || format!("Failed to parse address: {}", address)
        )?;
        println!("Starting gRPC server on {}", address);
        let server = TonicVegaFusionRuntimeServer::new(VegaFusionRuntimeGrpc::new(runtime));
        Server::builder().add_service(server).serve(addr).await?;
    } else {
        // Nothing to do
    }
    Ok(())
}

async fn websocket_server(
    address: Option<String>,
    runtime: TaskGraphRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(address) = address {
        // Create the event loop and TCP listener we'll accept connections on.
        let try_socket = TcpListener::bind(&address).await;
        let listener = try_socket.expect("Failed to bind");
        println!("Starting WebSocket server on: {}", address);

        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(accept_websocket_connection(stream, runtime.clone()));
        }
    } else {
        // Nothing to do
    }

    Ok(())
}

async fn accept_websocket_connection(
    stream: TcpStream,
    runtime: TaskGraphRuntime,
) -> Result<(), VegaFusionError> {
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