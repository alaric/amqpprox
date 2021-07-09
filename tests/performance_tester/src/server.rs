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

use amq_protocol::frame::AMQPFrame;
use amq_protocol::frame::GenError;
use amq_protocol::frame::WriteContext;
use amq_protocol::protocol::channel;
use amq_protocol::protocol::connection;
use amq_protocol::protocol::AMQPClass;
use amq_protocol::types::AMQPValue;
use anyhow::Result;
use anyhow::bail;
use bytes::BytesMut;
use futures::SinkExt;
use futures::StreamExt;
use std::io::Cursor;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use AMQPFrame::Method;

#[derive(thiserror::Error, Debug)]
enum AMQPCodecError {
    #[error("Underlying Error: {0}")]
    Underlying(String),

    #[error("Generate error")]
    GenError(#[from] GenError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<std::io::Error> for AMQPCodecError {
    fn from(error: std::io::Error) -> AMQPCodecError {
        AMQPCodecError::Underlying(error.to_string())
    }
}

struct AMQPCodec {}

impl tokio_util::codec::Decoder for AMQPCodec {
    type Item = AMQPFrame;
    type Error = AMQPCodecError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        log::trace!("Attempt decode from: {} bytes", src.len());
        let (consumed, res) = match amq_protocol::frame::parse_frame(src) {
            Ok((consumed, frame)) => (src.len() - consumed.len(), Ok(Some(frame))),
            Err(e) => {
                if e.is_incomplete() {
                    (0, Ok(None))
                } else {
                    (
                        0,
                        Err(AMQPCodecError::Underlying(format!(
                            "Parse error for frame: {:?}",
                            e
                        ))),
                    )
                }
            }
        };

        log::trace!("Consumed: {}, Res: {:?}", consumed, res);
        let _ = src.split_to(consumed);
        res
    }
}

impl tokio_util::codec::Encoder<AMQPFrame> for AMQPCodec {
    type Error = AMQPCodecError;
    fn encode(&mut self, item: AMQPFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        loop {
            let res = amq_protocol::frame::gen_frame(&item)(WriteContext::from(Cursor::new(
                dst.as_mut(),
            )));
            match res {
                Ok(wc) => {
                    let (_writer, position) = wc.into_inner();
                    dst.resize(position as usize, 0);
                    return Ok(());
                }
                Err(amq_protocol::frame::GenError::BufferTooSmall(sz)) => {
                    let capacity = dst.capacity();
                    dst.resize(capacity + sz, 0);
                }
                Err(e) => {
                    return Err(e.into());
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
    let mut buf: [u8; 8] = [0; 8];
    socket.read_exact(&mut buf).await?; // We ignore if it's correct

    log::info!("Protocol header received");

    let codec = AMQPCodec::new();
    let mut framed = tokio_util::codec::Framed::new(socket, codec);

    let mut server_props = amq_protocol::types::FieldTable::default();
    server_props.insert(
        String::from("product").into(),
        AMQPValue::LongString(String::from("amqpprox_perf_test").into()),
    );

    let start_method = connection::AMQPMethod::Start(connection::Start {
        version_major: 0,
        version_minor: 9,
        mechanisms: "PLAIN".to_string().into(),
        locales: "en_US".to_string().into(),
        server_properties: server_props,
    });
    let start = Method(0, AMQPClass::Connection(start_method));
    framed.send(start).await?;

    let frame = framed.next().await;
    if let Some(Ok(Method(0, AMQPClass::Connection(connection::AMQPMethod::StartOk(frame))))) =
        frame
    {
        log::debug!("Should be start-ok: {:?}", frame);
        let tune_method = connection::AMQPMethod::Tune(connection::Tune {
            channel_max: 2047,
            frame_max: 131072,
            heartbeat: 60,
        });
        let tune = Method(0, AMQPClass::Connection(tune_method));
        framed.send(tune).await?;
    } else {
        bail!("Invalid protocol, received: {:?}", frame);
    }

    let frame = framed.next().await;
    if let Some(Ok(Method(0, AMQPClass::Connection(connection::AMQPMethod::TuneOk(frame))))) = frame
    {
        log::debug!("Should be tune-ok: {:?}", frame);
    } else {
        bail!("Invalid protocol, received: {:?}", frame);
    }

    let frame = framed.next().await;
    if let Some(Ok(Method(0, AMQPClass::Connection(connection::AMQPMethod::Open(frame))))) = frame {
        log::debug!("Should be open: {:?}", frame);
        let openok_method = connection::AMQPMethod::OpenOk(connection::OpenOk {});
        let openok = Method(0, AMQPClass::Connection(openok_method));
        framed.send(openok).await?;
    } else {
        bail!("Invalid protocol, received: {:?}", frame);
    }

    log::info!("Connected!");
    while let Some(frame) = framed.next().await {
        log::trace!("Received: {:?}", &frame);
        match frame {
            Ok(Method(channel, AMQPClass::Channel(channelmsg))) => {
                log::debug!("Set up channel: {} {:?}", channel, channelmsg);
                let channelok_method = channel::AMQPMethod::OpenOk(channel::OpenOk {});
                let channelok = Method(channel, AMQPClass::Channel(channelok_method));
                framed.send(channelok).await?;
            }
            Ok(Method(0, AMQPClass::Connection(connection::AMQPMethod::Close(closemsg)))) => {
                log::info!("Closing connection requested: {:?}", closemsg);
                let closeok_method = connection::AMQPMethod::CloseOk(connection::CloseOk {});
                let closeok = Method(0, AMQPClass::Connection(closeok_method));
                framed.send(closeok).await?;
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
        let (socket, peer) = listener.accept().await?;
        log::debug!("Connection from {}", peer);
        tokio::spawn(async move { process_connection(socket).await });
    }
}
