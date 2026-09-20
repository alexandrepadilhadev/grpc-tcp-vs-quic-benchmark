"""Transport factory for runtime TCP or QUIC selection."""

from src.transport.quic.client import QuicClient
from src.transport.quic.server import QuicServer
from src.transport.tcp.client import TcpClient
from src.transport.tcp.server import TcpServer
from src.logger import create_logger


def create_server(config, service, logger=None):
    """Create the configured server implementation."""
    logger = logger or create_logger("server", config)
    if config.app_transport == "tcp":
        return TcpServer(config, service, logger)
    if config.app_transport == "quic":
        return QuicServer(config, service, logger)
    raise ValueError(f"Unsupported transport: {config.app_transport}")


def create_client(config, logger=None):
    """Create the configured client implementation."""
    logger = logger or create_logger("client", config)
    if config.app_transport == "tcp":
        return TcpClient(config, logger)
    if config.app_transport == "quic":
        return QuicClient(config, logger)
    raise ValueError(f"Unsupported transport: {config.app_transport}")
