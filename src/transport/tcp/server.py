"""gRPC over HTTP/2 server implementation."""

import asyncio
from pathlib import Path


import grpc

from src.generated import payment_pb2_grpc
from src.logger import StructuredLogger
from src.service.payment_service import PaymentService


class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    """Adapt the transport-neutral payment service to grpc.aio."""

    def __init__(self, service: PaymentService, logger: StructuredLogger):
        self.service = service
        self.logger = logger

    async def ProcessPayment(self, request, context):
        """Handle a unary payment request."""
        payload_size = request.ByteSize()
        try:
            with self.logger.track_rpc(
                "ProcessPayment", "TCP_HTTP2", payload_size
            ):
                return await self.service.process_payment(request)
        except Exception as exc:
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))

    async def StreamPaymentStatus(self, request, context):
        """Handle a server-streaming payment status request."""
        payload_size = request.ByteSize()
        try:
            with self.logger.track_rpc(
                "StreamPaymentStatus", "TCP_HTTP2", payload_size
            ):
                async for update in self.service.stream_payment_status(request):
                    yield update
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await context.abort(grpc.StatusCode.INTERNAL, str(exc))


class TcpServer:
    """Lifecycle wrapper for a secure grpc.aio server."""

    def __init__(self, config, service: PaymentService, logger: StructuredLogger):
        self.config = config
        self.service = service
        self.logger = logger
        self.server = grpc.aio.server()
        payment_pb2_grpc.add_PaymentServiceServicer_to_server(
            PaymentServicer(service, logger), self.server
        )
        self._configure_credentials()

    def _configure_credentials(self) -> None:
        cert_path = Path(self.config.tls_cert_path)
        key_path = Path(self.config.tls_key_path)
        if not cert_path.is_file() or not key_path.is_file():
            raise FileNotFoundError(
                f"TLS certificate or key not found: {cert_path}, {key_path}"
            )
        credentials = grpc.ssl_server_credentials(
            ((key_path.read_bytes(), cert_path.read_bytes()),)
        )
        self.server.add_secure_port(self.config.get_server_url(), credentials)

    async def start(self) -> None:
        """Start serving requests."""
        await self.server.start()
        self.logger.info(
            f"TCP server listening on {self.config.get_server_url()}",
            transport="TCP_HTTP2",
        )

    async def wait_closed(self) -> None:
        """Wait until the server is stopped."""
        await self.server.wait_for_termination()

    async def stop(self, grace: float = 0) -> None:
        """Stop the server."""
        await self.server.stop(grace)
