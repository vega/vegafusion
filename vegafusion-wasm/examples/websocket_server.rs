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

    let runtime = TaskGraphRuntime::new(Some(64), Some(1024_i32.pow(3) as usize));

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
        if let Message::Binary(request_bytes) = msg {
            let response_bytes = task_graph_runtime
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
