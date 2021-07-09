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

use amiquip::{Connection, Exchange, Publish};
use anyhow::Result;
use std::net::SocketAddr;

pub(crate) fn start_client(sz: usize, address: SocketAddr) -> Result<()> {
    let mut connection = Connection::insecure_open(format!("amqp://{}", address).as_str())?;
    let channel = connection.open_channel(None)?;
    let exchange = Exchange::direct(&channel);

    let mut arr = Vec::new();
    arr.resize(sz, 0);
    loop {
        exchange.publish(Publish::new(&arr, "hello"))?;
    }
}
