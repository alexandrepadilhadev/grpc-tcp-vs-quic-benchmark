"""Server entry point for the dual-stack payment POC."""

import asyncio
import logging

from src.config import load_config
from src.logger import create_logger
from src.service.payment_service import PaymentService
from src.transport.factory import create_server


async def run_server() -> None:
    """Load configuration, start the selected transport, and serve requests."""
    config = load_config()
    logger = create_logger("server", config)
    service = PaymentService(
        failure_probability=config.payment_failure_probability,
        logger=logger,
        transport="TCP_HTTP2" if config.app_transport == "tcp" else "QUIC_HTTP3",
    )
    server = create_server(config, service, logger)

    await server.start()
    try:
        await server.wait_closed()
    finally:
        await server.stop()
        logger.info("Server stopped", transport=_transport_name(config.app_transport))


def _transport_name(transport: str) -> str:
    """Return the structured transport name used by the logger."""
    return "TCP_HTTP2" if transport == "tcp" else "QUIC_HTTP3"


def main() -> None:
    """Run the server entry point."""
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Server interrupted")


if __name__ == "__main__":
    main()
