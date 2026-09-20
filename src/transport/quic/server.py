"""Minimal gRPC-framed HTTP/3 server over QUIC."""

import asyncio
from collections import defaultdict
from functools import partial
from pathlib import Path

from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration

from src.generated import payment_pb2
from src.logger import StructuredLogger
from src.service.payment_service import PaymentService


GRPC_CONTENT_TYPE = "application/grpc+proto"


def encode_grpc_message(message) -> bytes:
    """Encode a protobuf message as a five-byte gRPC frame."""
    payload = message.SerializeToString()
    return b"\x00" + len(payload).to_bytes(4, "big") + payload


def decode_grpc_messages(buffer: bytearray):
    """Yield complete protobuf payloads from a gRPC frame buffer."""
    messages = []
    while len(buffer) >= 5:
        size = int.from_bytes(buffer[1:5], "big")
        if len(buffer) < 5 + size:
            break
        messages.append(bytes(buffer[5 : 5 + size]))
        del buffer[: 5 + size]
    return messages


class QuicGrpcServerProtocol(QuicConnectionProtocol):
    """Translate HTTP/3 requests into PaymentService calls."""

    def __init__(self, *args, service: PaymentService, logger: StructuredLogger, **kwargs):
        super().__init__(*args, **kwargs)
        self.service = service
        self.logger = logger
        self.http = H3Connection(self._quic)
        self.buffers = defaultdict(bytearray)
        self.headers = {}

    def quic_event_received(self, event) -> None:
        for http_event in self.http.handle_event(event):
            if isinstance(http_event, HeadersReceived):
                self.headers[http_event.stream_id] = dict(http_event.headers)
            elif isinstance(http_event, DataReceived):
                buffer = self.buffers[http_event.stream_id]
                buffer.extend(http_event.data)
                if http_event.stream_ended:
                    asyncio.create_task(self._handle_request(http_event.stream_id))
        self.transmit()

    async def _handle_request(self, stream_id: int) -> None:
        request_headers = self.headers.get(stream_id, {})
        path = request_headers.get(b":path", b"").decode()
        payloads = decode_grpc_messages(self.buffers[stream_id])
        if len(payloads) != 1:
            await self._send_error(stream_id, "INTERNAL", "Invalid gRPC frame")
            return

        started = asyncio.get_running_loop().time()
        try:
            if path.endswith("/ProcessPayment"):
                request = payment_pb2.PaymentRequest.FromString(payloads[0])
                response = await self.service.process_payment(request)
                await self._send_messages(
                    stream_id, "ProcessPayment", [response], len(payloads[0]), started
                )
            elif path.endswith("/StreamPaymentStatus"):
                request = payment_pb2.StreamStatusRequest.FromString(payloads[0])
                updates = [
                    update
                    async for update in self.service.stream_payment_status(request)
                ]
                await self._send_messages(
                    stream_id,
                    "StreamPaymentStatus",
                    updates,
                    len(payloads[0]),
                    started,
                )
            else:
                await self._send_error(stream_id, "UNIMPLEMENTED", "Unknown RPC path")
        except Exception as exc:
            await self._send_error(stream_id, "INTERNAL", str(exc))

    async def _send_messages(self, stream_id, method, messages, payload_size, started):
        self.http.send_headers(
            stream_id,
            [(b":status", b"200"), (b"content-type", GRPC_CONTENT_TYPE.encode())],
        )
        for message in messages:
            self.http.send_data(stream_id, encode_grpc_message(message), end_stream=False)
        self.http.send_headers(
            stream_id,
            [(b"grpc-status", b"0"), (b"grpc-message", b"")],
            end_stream=True,
        )
        self.transmit()
        duration_ms = (asyncio.get_running_loop().time() - started) * 1000
        self.logger.info(
            f"RPC {method} completed",
            transport="QUIC_HTTP3",
            rpc_method=method,
            duration_ms=duration_ms,
            status_code="OK",
            payload_size_bytes=payload_size,
        )

    async def _send_error(self, stream_id, status, message):
        self.http.send_headers(
            stream_id,
            [(b":status", b"200"), (b"content-type", GRPC_CONTENT_TYPE.encode())],
        )
        self.http.send_headers(
            stream_id,
            [(b"grpc-status", status.encode()), (b"grpc-message", message.encode())],
            end_stream=True,
        )
        self.transmit()


class QuicServer:
    """Lifecycle wrapper for the HTTP/3 QUIC server."""

    def __init__(self, config, service: PaymentService, logger: StructuredLogger):
        self.config = config
        self.service = service
        self.logger = logger
        self._server = None

    async def start(self) -> None:
        """Start the QUIC listener."""
        configuration = QuicConfiguration(is_client=False, alpn_protocols=["h3"])
        configuration.load_cert_chain(
            str(Path(self.config.tls_cert_path)), str(Path(self.config.tls_key_path))
        )
        protocol = partial(
            QuicGrpcServerProtocol, service=self.service, logger=self.logger
        )
        self._server = await serve(
            self.config.server_host,
            self.config.server_port,
            configuration=configuration,
            create_protocol=protocol,
        )
        self.logger.info(
            f"QUIC server listening on {self.config.get_server_url()}",
            transport="QUIC_HTTP3",
        )

    async def wait_closed(self) -> None:
        """Keep the server alive until cancelled."""
        await asyncio.Future()

    async def stop(self, grace: float = 0) -> None:
        """Close the QUIC listener."""
        del grace
        if self._server is not None:
            self._server.close()
            self._server = None
