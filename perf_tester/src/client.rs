use amiquip::{Connection, Exchange, Publish};
use std::net::SocketAddr;
use anyhow::Result;

pub(crate) fn start_client(sz: usize, address: SocketAddr) -> Result<()> {
    let mut connection = Connection::insecure_open(format!("amqp://{}", address).as_str())?;
    let channel = connection.open_channel(None)?;
    let exchange = Exchange::direct(&channel);

    let mut arr = Vec::new();
    arr.resize(sz, 0);
    loop {
        exchange.publish(Publish::new(&arr, "hello"))?;
    }

    connection.close()?;
    Ok(())
}
