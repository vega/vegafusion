//! A simple echo server.
//!
//! You can test this out by running:
//!
//!     cargo run --example server 127.0.0.1:12345
//!
//! And then in another window run:
//!
//!     cargo run --example client ws://127.0.0.1:12345/

use futures_util::{future, SinkExt, StreamExt, TryStreamExt};
use prost::Message as ProstMessage;
use std::convert::TryFrom;
use std::sync::Arc;
use std::{env, io::Error};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::services::{
    vega_fusion_runtime_request, vega_fusion_runtime_response, VegaFusionRuntimeRequest,
    VegaFusionRuntimeResponse,
};
use vegafusion_core::proto::gen::tasks::{
    ResponseTaskValue, TaskGraphValueResponse, TaskValue as ProtoTaskValue,
};
use vegafusion_rt_datafusion::task_graph::runtime::TaskGraphRuntime;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::result::Result<(), Error> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8087".to_string());

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", addr);

    let runtime = TaskGraphRuntime::new(10);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_connection(stream, runtime.clone()));
    }

    Ok(())
}

async fn accept_connection(stream: TcpStream, task_graph_runtime: TaskGraphRuntime) -> Result<()> {
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
        if let Message::Binary(bytes) = msg {
            // Decode request
            let request = VegaFusionRuntimeRequest::decode(bytes.as_slice()).unwrap();

            if let Some(vega_fusion_runtime_request::Request::TaskGraphValues(task_graph_values)) =
                request.request
            {
                let task_graph = Arc::new(task_graph_values.task_graph.unwrap());

                // Clone task_graph and task_graph_runtime for use in closure
                let task_graph_runtime = task_graph_runtime.clone();
                let task_graph = task_graph.clone();

                // println!("task_graph_values.indices: {:?}", task_graph_values.indices);
                let response_value_futures: Vec<_> = task_graph_values
                    .indices
                    .iter()
                    .map(|node_value_index| {
                        let node = &task_graph.nodes[node_value_index.node_index as usize];
                        let task = node.task();
                        let var = match node_value_index.output_index {
                            None => task.variable().clone(),
                            Some(output_index) => task.output_vars()[output_index as usize].clone(),
                        };

                        let scope = node.task().scope.clone();

                        // Clone task_graph and task_graph_runtime for use in closure
                        let task_graph_runtime = task_graph_runtime.clone();
                        let task_graph = task_graph.clone();

                        async move {
                            let value = task_graph_runtime
                                .clone()
                                .get_node_value(task_graph, &node_value_index)
                                .await
                                .expect(&format!("var {:?}", var));

                            ResponseTaskValue {
                                variable: Some(var),
                                scope,
                                value: Some(ProtoTaskValue::try_from(&value).unwrap()),
                            }
                        }
                    })
                    .collect();

                let response_values = future::join_all(response_value_futures).await;

                // Return empty response for now
                let response_msg = VegaFusionRuntimeResponse {
                    response: Some(vega_fusion_runtime_response::Response::TaskGraphValues(
                        TaskGraphValueResponse { response_values },
                    )),
                };

                // println!("response_msg: {:?}", response_msg);

                let mut buf: Vec<u8> = Vec::new();
                buf.reserve(response_msg.encoded_len());
                response_msg.encode(&mut buf).unwrap();

                let response = Message::binary(buf);
                ws_stream
                    .send(response)
                    .await
                    .or_else(|_| Err(VegaFusionError::internal("websocket response failed")))?;
            }
        } else {
            return Err(VegaFusionError::internal("expected binary message"));
        }
    }

    Ok(())
}
