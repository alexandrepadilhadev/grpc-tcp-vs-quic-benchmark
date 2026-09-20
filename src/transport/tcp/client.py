"""gRPC over HTTP/2 client implementation."""

from pathlib import Path

import grpc

from src.generated import payment_pb2, payment_pb2_grpc
from src.logger import StructuredLogger


class TcpClient:
    """Async secure gRPC client for the payment service."""

    def __init__(self, config, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.channel = None
        self.stub = None

    async def connect(self) -> None:
        """Open a TLS-secured HTTP/2 channel."""
        cert_path = Path(self.config.tls_cert_path)
        if not cert_path.is_file():
            raise FileNotFoundError(f"TLS certificate not found: {cert_path}")
        credentials = grpc.ssl_channel_credentials(cert_path.read_bytes())
        self.channel = grpc.aio.secure_channel(
            self.config.get_server_url(), credentials
        )
        self.stub = payment_pb2_grpc.PaymentServiceStub(self.channel)
        await self.channel.channel_ready()


    async def process_payment(self, request: payment_pb2.PaymentRequest):
        """Call the unary ProcessPayment RPC."""
        if self.stub is None:
            raise RuntimeError("Client is not connected")
        with self.logger.track_rpc(
            "ProcessPayment", "TCP_HTTP2", request.ByteSize()
        ):
            return await self.stub.ProcessPayment(request)

    async def stream_payment_status(self, request: payment_pb2.StreamStatusRequest):
        """Return the async response stream for StreamPaymentStatus."""
        if self.stub is None:
            raise RuntimeError("Client is not connected")
        start = self.logger.track_rpc(
            "StreamPaymentStatus", "TCP_HTTP2", request.ByteSize()
        )
        with start:
            call = self.stub.StreamPaymentStatus(request)
            async for update in call:
                yield update

    async def close(self) -> None:
        """Close the channel."""
        if self.channel is not None:
            await self.channel.close()
            self.channel = None
            self.stub = None
