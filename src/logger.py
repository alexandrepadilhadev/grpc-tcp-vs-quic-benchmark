"""
Structured JSON file logging for the gRPC POC.

Provides JSON formatted logs to logs/server.log and logs/client.log with
standardized fields: timestamp, level, transport, rpc_method, duration_ms,
status_code, and payload_size_bytes.
"""

import json
import logging
import logging.handlers
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

from src.config import Config


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs structured JSON log records."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: The log record to format.

        Returns:
            JSON string representation of the log record.
        """
        # RFC3339/ISO8601 timestamp with millisecond precision
        timestamp = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).isoformat(timespec="milliseconds")

        # Base log entry
        log_entry = {
            "timestamp": timestamp,
            "level": record.levelname,
        }

        # Add extra fields if present (transport, rpc_method, duration_ms, etc.)
        if hasattr(record, "transport"):
            log_entry["transport"] = record.transport
        if hasattr(record, "rpc_method"):
            log_entry["rpc_method"] = record.rpc_method
        if hasattr(record, "duration_ms"):
            log_entry["duration_ms"] = record.duration_ms
        if hasattr(record, "status_code"):
            log_entry["status_code"] = record.status_code
        if hasattr(record, "payload_size_bytes"):
            log_entry["payload_size_bytes"] = record.payload_size_bytes

        # Add message
        log_entry["message"] = record.getMessage()

        return json.dumps(log_entry, separators=(",", ":"))


class StructuredLogger:
    """Wrapper around Python logger for structured JSON logging."""

    def __init__(self, name: str, log_file: Path):
        """
        Initialize a structured logger.

        Args:
            name: Logger name (e.g., 'server', 'client').
            log_file: Path to the log file.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # File handler with JSON formatter
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
        )
        file_handler.setLevel(logging.DEBUG)
        formatter = JSONFormatter()
        file_handler.setFormatter(formatter)

        # Clear existing handlers and add our handler
        self.logger.handlers = [file_handler]

    def _log(
        self,
        level: int,
        message: str,
        transport: Optional[str] = None,
        rpc_method: Optional[str] = None,
        duration_ms: Optional[float] = None,
        status_code: Optional[str] = None,
        payload_size_bytes: Optional[int] = None,
    ) -> None:
        """
        Log a message with optional structured fields.

        Args:
            level: Log level (e.g., logging.INFO).
            message: Log message.
            transport: Transport protocol (TCP_HTTP2 or QUIC_HTTP3).
            rpc_method: RPC method name.
            duration_ms: Duration in milliseconds.
            status_code: gRPC status code.
            payload_size_bytes: Message payload size.
        """
        extra: dict[str, Any] = {}
        if transport is not None:
            extra["transport"] = transport
        if rpc_method is not None:
            extra["rpc_method"] = rpc_method
        if duration_ms is not None:
            extra["duration_ms"] = round(duration_ms, 2)
        if status_code is not None:
            extra["status_code"] = status_code
        if payload_size_bytes is not None:
            extra["payload_size_bytes"] = payload_size_bytes

        self.logger.log(level, message, extra=extra)

    def debug(
        self,
        message: str,
        transport: Optional[str] = None,
        rpc_method: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Log at DEBUG level."""
        self._log(logging.DEBUG, message, transport, rpc_method, **kwargs)

    def info(
        self,
        message: str,
        transport: Optional[str] = None,
        rpc_method: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Log at INFO level."""
        self._log(logging.INFO, message, transport, rpc_method, **kwargs)

    def warning(
        self,
        message: str,
        transport: Optional[str] = None,
        rpc_method: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Log at WARN level."""
        self._log(logging.WARNING, message, transport, rpc_method, **kwargs)

    def error(
        self,
        message: str,
        transport: Optional[str] = None,
        rpc_method: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Log at ERROR level."""
        self._log(logging.ERROR, message, transport, rpc_method, **kwargs)

    @contextmanager
    def track_rpc(
        self,
        rpc_method: str,
        transport: str,
        payload_size_bytes: Optional[int] = None,
    ) -> Generator[None, None, None]:
        """
        Context manager to track RPC execution time and log results.

        Args:
            rpc_method: Name of the RPC method being called.
            transport: Transport protocol (TCP_HTTP2 or QUIC_HTTP3).
            payload_size_bytes: Optional payload size in bytes.

        Yields:
            Control to caller for RPC execution.
        """
        start_time = time.time()
        status_code = "OK"

        try:
            yield
        except Exception as e:
            status_code = "ERROR"
            duration_ms = (time.time() - start_time) * 1000
            self.error(
                f"RPC {rpc_method} failed: {str(e)}",
                transport=transport,
                rpc_method=rpc_method,
                duration_ms=duration_ms,
                status_code=status_code,
                payload_size_bytes=payload_size_bytes,
            )
            raise
        else:
            duration_ms = (time.time() - start_time) * 1000
            self.info(
                f"RPC {rpc_method} completed",
                transport=transport,
                rpc_method=rpc_method,
                duration_ms=duration_ms,
                status_code=status_code,
                payload_size_bytes=payload_size_bytes,
            )


def create_logger(name: str, config: Config) -> StructuredLogger:
    """
    Create a structured logger for the application.

    Args:
        name: Logger identifier ('server' or 'client').
        config: Configuration object.

    Returns:
        StructuredLogger instance configured for the name and config.
    """
    log_file = config.log_dir / f"{name}.log"
    logger = StructuredLogger(name, log_file)
    logger.info(
        f"{name.capitalize()} logger initialized",
        transport=config.app_transport.upper() + "_HTTP2"
        if config.app_transport == "tcp"
        else "QUIC_HTTP3",
    )
    return logger
