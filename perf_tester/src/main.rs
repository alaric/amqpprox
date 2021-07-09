/*
** Copyright 2021 Bloomberg Finance L.P.
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/

use std::path::PathBuf;
use structopt::StructOpt;
use std::net::SocketAddr;
use anyhow::Result;

mod client;
mod server;

#[derive(Debug, StructOpt)]
struct PerfTesterOpts {
    #[structopt(long)]
    address: SocketAddr,

    #[structopt(long)]
    server_address: SocketAddr,

    #[structopt(long)]
    control_sockets: Vec<PathBuf>,

    #[structopt(long)]
    clients: u32,

    #[structopt(long)]
    message_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let opts = PerfTesterOpts::from_args();
    log::info!("Starting performance test of amqpprox");

    let address = opts.server_address.clone();
    let server = tokio::spawn(async move { server::run_server(address).await });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await; // TODO 

    let mut handles = Vec::new();
    for _ in 0..opts.clients {
        let address = opts.address.clone();
        let message_size = opts.message_size;
        let handle = tokio::task::spawn_blocking(move || { crate::client::start_client(message_size, address) });
        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    server.await??;
    Ok(())
}
