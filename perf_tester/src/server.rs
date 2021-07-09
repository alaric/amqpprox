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

use anyhow::Result;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use bytes::BytesMut;
use futures::StreamExt;
use futures::SinkExt;

#[derive(Debug)]
enum AMQPCodecError {
    Underlying(String),
}

impl From<std::io::Error> for AMQPCodecError {
    fn from(error: std::io::Error) -> AMQPCodecError {
        AMQPCodecError::Underlying(error.to_string())
    }
}

struct AMQPCodec {}

impl tokio_util::codec::Decoder for AMQPCodec {
    type Item = amq_protocol::frame::AMQPFrame;
    type Error = AMQPCodecError;
    fn decode(
        &mut self, 
        src: &mut BytesMut
    ) -> Result<Option<Self::Item>, Self::Error> {
        log::trace!("Attempt decode from: {} bytes", src.len());
        let (consumed, res) = match amq_protocol::frame::parse_frame(src) {
            Ok((consumed, frame)) => {
                (src.len() - consumed.len(), Ok(Some(frame)))
            },
            Err(e) => {
                if e.is_incomplete() {
                    (0, Ok(None))
                }
                else {
                    (0, Err(AMQPCodecError::Underlying(format!("Parse error for frame: {:?}", e))))
                }
            },
        };

        log::trace!("Consumed: {}, Res: {:?}", consumed, res);
        let _ = src.split_to(consumed);
        res
    }
}

impl tokio_util::codec::Encoder<amq_protocol::frame::AMQPFrame> for AMQPCodec {
    type Error = AMQPCodecError;
    fn encode(
        &mut self, 
        item: amq_protocol::frame::AMQPFrame, 
        dst: &mut BytesMut
    ) -> Result<(), Self::Error> {
        loop {

        let res = amq_protocol::frame::gen_frame(&item)(amq_protocol::frame::WriteContext::from(std::io::Cursor::new(dst.as_mut())));
        match res {
            Ok(wc) => { 
                let (_writer, position) = wc.into_inner();
                //log::info!("WC={:?}", wc.into_inner());
                dst.resize(position as usize, 0);
                return Ok(()); },
             Err(amq_protocol::frame::GenError::BufferTooSmall(sz)) => {
                let capacity = dst.capacity();
                dst.resize(capacity + sz, 0);
                log::info!("Doubling capacity to {}", capacity + sz);
             },
            Err(e) => {
                log::error!("Write error: {:?}", e);
                return Ok((()));
            }
        }
        }
    }
}

impl AMQPCodec {
    fn new() -> Self {
        Self {}
    }
}

pub async fn process_connection(mut socket: TcpStream) -> Result<()> {
    //socket.write_all("AMQP\x00\x00\x09\x01".as_bytes()).await?;
    let mut buf: [u8; 8] = [0; 8];
    socket.read_exact(&mut buf).await?;

    log::info!("Protocol header received");

    let codec = AMQPCodec::new();
    let mut framed = tokio_util::codec::Framed::new(socket, codec);

    let mut server_props = amq_protocol::types::FieldTable::default();
    server_props.insert(String::from("product").into(), amq_protocol::types::AMQPValue::LongString(String::from("amqpprox_perf_test").into()));

    let start_method = amq_protocol::protocol::connection::AMQPMethod::Start(amq_protocol::protocol::connection::Start {
        version_major: 0,
        version_minor: 9,
        mechanisms: "PLAIN".to_string().into(),
        locales: "en_US".to_string().into(),
        server_properties: server_props,
    });
    let start = amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(start_method));
    let res = framed.send(start).await;

    let frame = framed.next().await;
    if let Some(Ok(amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(amq_protocol::protocol::connection::AMQPMethod::StartOk(frame))))) = frame {
        log::info!("Should be start-ok: {:?}", frame);
        let tune_method = amq_protocol::protocol::connection::AMQPMethod::Tune(amq_protocol::protocol::connection::Tune {
            channel_max: 2047,
            frame_max: 131072,
            heartbeat: 60,
        });
        let tune = amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(tune_method));
        let res = framed.send(tune).await;
    }
    else {
        anyhow::bail!("Invalid protocol, received: {:?}", frame);
    }

    let frame = framed.next().await;
    if let Some(Ok(amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(amq_protocol::protocol::connection::AMQPMethod::TuneOk(frame))))) = frame {
        log::info!("Should be tune-ok: {:?}", frame);
    }
    else {
        anyhow::bail!("Invalid protocol, received: {:?}", frame);
    }

    let frame = framed.next().await;
    if let Some(Ok(amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(amq_protocol::protocol::connection::AMQPMethod::Open(frame))))) = frame {
        log::info!("Should be open: {:?}", frame);
        // TODO send openok
        let openok_method = amq_protocol::protocol::connection::AMQPMethod::OpenOk(amq_protocol::protocol::connection::OpenOk {
        });
        let openok = amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(openok_method));
        let res = framed.send(openok).await;
    }
    else {
        anyhow::bail!("Invalid protocol, received: {:?}", frame);
    }

    log::info!("Connected!");
    while let Some(frame) = framed.next().await {
        log::info!("Received: {:?}", &frame);
        match frame {
            Ok(amq_protocol::frame::AMQPFrame::Method(channel, amq_protocol::protocol::AMQPClass::Channel(channelmsg))) => {
                log::info!("Set up channel: {} {:?}", channel, channelmsg);
                let channelok_method = amq_protocol::protocol::channel::AMQPMethod::OpenOk(amq_protocol::protocol::channel::OpenOk {
                });
                let channelok = amq_protocol::frame::AMQPFrame::Method(channel, amq_protocol::protocol::AMQPClass::Channel(channelok_method));
                framed.send(channelok).await;
            }
            Ok(amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(amq_protocol::protocol::connection::AMQPMethod::Close(closemsg)))) => {
                log::info!("Closing connection requested: {:?}", closemsg);
                let closeok_method = amq_protocol::protocol::connection::AMQPMethod::CloseOk(amq_protocol::protocol::connection::CloseOk {
                });
                let closeok = amq_protocol::frame::AMQPFrame::Method(0, amq_protocol::protocol::AMQPClass::Connection(closeok_method));
                framed.send(closeok).await;
                return Ok(());
            }
            _ => {}
        }
    }
    Ok(())
}

pub async fn run_server(address: SocketAddr) -> Result<()> {

    log::info!("Awaiting on {:?}", &address);
    let listener = TcpListener::bind(address).await?;

    loop {
        log::info!("About to accept");
        let (socket, peer) = listener.accept().await?;
        log::info!("Connection from {}", peer);
        tokio::spawn(async move {
            process_connection(socket).await
        });
    }
}
