"""Minimal gRPC-framed HTTP/3 client over QUIC."""

import asyncio
import os
import ssl
from collections import defaultdict
from pathlib import Path


from aioquic.asyncio import QuicConnectionProtocol, connect
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration

from src.generated import payment_pb2
from src.logger import StructuredLogger
from src.transport.quic.server import decode_grpc_messages, encode_grpc_message

sslkeylog_path = os.getenv("SSLKEYLOGFILE")

class QuicGrpcClientProtocol(QuicConnectionProtocol):
    """HTTP/3 protocol that queues response frames by stream ID."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http = H3Connection(self._quic)
        self.responses = defaultdict(bytearray)
        self.queues = defaultdict(asyncio.Queue)
        self.headers = {}

    def quic_event_received(self, event) -> None:
        for http_event in self.http.handle_event(event):
            if isinstance(http_event, HeadersReceived):
                self.headers[http_event.stream_id] = dict(http_event.headers)
                if http_event.stream_ended:
                    self.queues[http_event.stream_id].put_nowait(None)
            elif isinstance(http_event, DataReceived):
                buffer = self.responses[http_event.stream_id]
                buffer.extend(http_event.data)
                for payload in decode_grpc_messages(buffer):
                    self.queues[http_event.stream_id].put_nowait(payload)
                if http_event.stream_ended:
                    self.queues[http_event.stream_id].put_nowait(None)
        self.transmit()


class QuicClient:
    """Async HTTP/3 client using protobuf gRPC framing."""

    def __init__(self, config, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.protocol = None
        self._connection = None

    async def connect(self) -> None:
        """Open a QUIC connection with TLS certificate verification disabled for the POC."""
        configuration = QuicConfiguration(is_client=True, alpn_protocols=["h3"])
        cert_path = Path(self.config.tls_cert_path)
        if not cert_path.is_file():
            raise FileNotFoundError(f"TLS certificate not found: {cert_path}")
        if sslkeylog_path:
            configuration.secrets_log_file = open(sslkeylog_path, "a", encoding="utf-8")
        configuration.load_verify_locations(str(cert_path))
        configuration.verify_mode = ssl.CERT_REQUIRED
        configuration.server_name = "localhost"
        self._connection = connect(
            self.config.server_host,
            self.config.server_port,
            configuration=configuration,
            create_protocol=QuicGrpcClientProtocol,
        )
        self.protocol = await self._connection.__aenter__()

    async def _call(self, path: str, request, response_type):
        if self.protocol is None:
            raise RuntimeError("Client is not connected")
        stream_id = self.protocol._quic.get_next_available_stream_id()
        queue = self.protocol.queues[stream_id]
        self.protocol.http.send_headers(
            stream_id,
            [
                (b":method", b"POST"),
                (b":scheme", b"https"),
                (b":authority", self.config.get_server_url().encode()),
                (b":path", path.encode()),
                (b"content-type", b"application/grpc+proto"),
                (b"te", b"trailers"),
            ],
        )
        self.protocol.http.send_data(
            stream_id, encode_grpc_message(request), end_stream=True
        )
        self.protocol.transmit()
        while True:
            payload = await queue.get()
            if payload is None:
                break
            yield response_type.FromString(payload)

    async def process_payment(self, request: payment_pb2.PaymentRequest):
        """Call ProcessPayment over HTTP/3."""
        with self.logger.track_rpc(
            "ProcessPayment", "QUIC_HTTP3", request.ByteSize()
        ):
            responses = [
                response
                async for response in self._call(
                    "/payment.PaymentService/ProcessPayment",
                    request,
                    payment_pb2.PaymentResponse,
                )
            ]
        if not responses:
            raise RuntimeError("QUIC server returned no payment response")
        return responses[0]

    async def stream_payment_status(self, request: payment_pb2.StreamStatusRequest):
        """Stream payment status updates over HTTP/3."""
        with self.logger.track_rpc(
            "StreamPaymentStatus", "QUIC_HTTP3", request.ByteSize()
        ):
            async for update in self._call(
                "/payment.PaymentService/StreamPaymentStatus",
                request,
                payment_pb2.PaymentStatusUpdate,
            ):
                yield update

    async def close(self) -> None:
        """Close the QUIC connection."""
        if self._connection is not None:
            await self._connection.__aexit__(None, None, None)
            self._connection = None
            self.protocol = None
