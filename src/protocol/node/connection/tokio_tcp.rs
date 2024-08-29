use std::task::ready;

use bytes::{Bytes, BytesMut};
use futures_util::{Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::protocol::node::{
    event::{N2NPayloadKind, N2nPacketHeader, N2nPacketId},
    N2nPacket,
};

use super::{NodeConnection, NodeConnectionError, NodeConnectionErrorKind};
#[derive(Debug, PartialEq, Eq)]
pub enum ReadState {
    ExpectingHeader,
    ExpectingPayload,
}
#[derive(Debug, PartialEq, Eq)]
pub enum WriteState {
    Ready,
    WritingHeader,
    WritingPayload,
}

const HEADER_SIZE: usize = std::mem::size_of::<N2nPacketHeader>();
pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct TokioTcp {
        #[pin]
        inner: tokio::net::TcpStream,
        // read buffers
        read_state: ReadState,
        read_header_buf: [u8; HEADER_SIZE],
        read_payload_buf: BytesMut,
        read_index: usize,
        read_header: Option<N2nPacketHeader>,
        // write buffers
        write_item: Option<N2nPacket>,
        write_header_buf: [u8; HEADER_SIZE],
        write_payload_buf: Bytes,
        write_state: WriteState,
        write_index: usize,
    }
}

impl TokioTcp {
    pub fn new_std(inner: std::net::TcpStream) -> std::io::Result<Self> {
        Ok(Self::new(tokio::net::TcpStream::from_std(inner)?))
    }
    pub fn new(inner: tokio::net::TcpStream) -> Self {
        Self {
            inner,
            read_state: ReadState::ExpectingHeader,
            read_header_buf: Default::default(),
            read_payload_buf: BytesMut::new(),
            read_index: 0,
            read_header: None,
            write_item: None,
            write_state: WriteState::Ready,
            write_index: 0,
            write_header_buf: Default::default(),
            write_payload_buf: Bytes::new(),
        }
    }
}

impl Sink<N2nPacket> for TokioTcp {
    type Error = NodeConnectionError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        let inner = this.inner;
        inner.poll_write_ready(cx).map_err(|e| {
            NodeConnectionError::new(NodeConnectionErrorKind::Io(e), "failed to poll ready")
        })
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: N2nPacket) -> Result<(), Self::Error> {
        let this = self.project();
        let header = item.header;
        this.write_header_buf[0..16].copy_from_slice(&header.id.bytes);
        this.write_header_buf[16] = header.kind as u8;
        this.write_header_buf[17..21].copy_from_slice(&header.payload_size.to_be_bytes());
        *this.write_payload_buf = item.payload;
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        let mut inner = this.inner;
        loop {
            match this.write_state {
                WriteState::Ready => {
                    *this.write_state = WriteState::WritingHeader;
                }
                WriteState::WritingHeader => {
                    let written = ready!(inner
                        .as_mut()
                        .poll_write(cx, &this.write_header_buf[(*this.write_index)..]))
                    .map_err(|e| {
                        NodeConnectionError::new(
                            NodeConnectionErrorKind::Io(e),
                            "failed to write header",
                        )
                    })?;
                    *this.write_index += written;
                    if *this.write_index == HEADER_SIZE {
                        *this.write_state = WriteState::WritingPayload;
                        *this.write_index = 0;
                    }
                }
                WriteState::WritingPayload => {
                    let written = ready!(inner
                        .as_mut()
                        .poll_write(cx, &this.write_payload_buf[(*this.write_index)..]))
                    .map_err(|e| {
                        NodeConnectionError::new(
                            NodeConnectionErrorKind::Io(e),
                            "failed to write payload",
                        )
                    })?;
                    *this.write_index += written;
                    if *this.write_index == this.write_payload_buf.len() {
                        *this.write_state = WriteState::Ready;
                        *this.write_index = 0;

                        return std::task::Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        let inner = this.inner;
        inner.poll_shutdown(cx).map_err(|e| {
            NodeConnectionError::new(NodeConnectionErrorKind::Io(e), "failed to shutdown")
        })
    }
}

impl Stream for TokioTcp {
    type Item = Result<N2nPacket, NodeConnectionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let mut inner = this.inner;
        loop {
            match this.read_state {
                ReadState::ExpectingHeader => {
                    let mut buffer = ReadBuf::new(&mut this.read_header_buf[(*this.read_index)..]);
                    let poll_read_result = inner.as_mut().poll_read(cx, &mut buffer).map_err(|e| {
                        NodeConnectionError::new(
                            NodeConnectionErrorKind::Io(e),
                            "failed to read header",
                        )
                    });
                    ready!(poll_read_result)?;
                    let remaining = buffer.remaining();
                    if remaining == 0 {
                        *this.read_state = ReadState::ExpectingPayload;
                        *this.read_index = 0;
                        let id = N2nPacketId {
                            bytes: (&this.read_header_buf[0..16])
                                .try_into()
                                .expect("have enough bytes"),
                        };
                        let kind = N2NPayloadKind::from(this.read_header_buf[16]);
                        let payload_size = u32::from_be_bytes(
                            (&this.read_header_buf[17..21])
                                .try_into()
                                .expect("have enough bytes"),
                        );
                        let header = N2nPacketHeader {
                            id,
                            kind,
                            payload_size,
                        };
                        this.read_payload_buf.reserve(payload_size as usize);
                        unsafe {
                            this.read_payload_buf.set_len(payload_size as usize);
                        }
                        *this.read_header = Some(header);
                    } else {
                        let new_index = HEADER_SIZE - remaining;
                        *this.read_index = new_index;
                    }
                }
                ReadState::ExpectingPayload => {
                    let header = this.read_header.as_ref().expect("header is set");
                    let payload_size = header.payload_size as usize;
                    if payload_size == 0 {
                        let header = this.read_header.take().expect("header is set");
                        *this.read_state = ReadState::ExpectingHeader;
                        return std::task::Poll::Ready(Some(Ok(N2nPacket {
                            header,
                            payload: Bytes::new(),
                        })));
                    }
                    let mut buffer = ReadBuf::new(&mut this.read_payload_buf[(*this.read_index)..]);
                    ready!(inner.as_mut().poll_read(cx, &mut buffer)).map_err(|e| {
                        NodeConnectionError::new(
                            NodeConnectionErrorKind::Io(e),
                            "failed to read payload",
                        )
                    })?;
                    let remain = buffer.remaining();
                    if remain == 0 {
                        let header = this.read_header.take().expect("header is set");
                        let payload = this.read_payload_buf.split().freeze();
                        *this.read_state = ReadState::ExpectingHeader;
                        *this.read_index = 0;
                        return std::task::Poll::Ready(Some(Ok(N2nPacket { header, payload })));
                    } else {
                        let new_index = payload_size - remain;
                        *this.read_index = new_index;
                    }
                }
            }
        }
    }
}

impl NodeConnection for TokioTcp {}
