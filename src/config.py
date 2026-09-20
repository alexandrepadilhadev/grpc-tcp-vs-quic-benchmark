"""
Configuration management for the gRPC POC application.

Loads and validates environment variables using Pydantic Settings.
Supports both TCP and QUIC transports with seamless switching via APP_TRANSPORT.
"""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """Application configuration loaded from environment variables and .env file."""

    # Transport mode: "tcp" for gRPC over HTTP/2, "quic" for gRPC over HTTP/3
    app_transport: Literal["tcp", "quic"] = Field(
        default="tcp",
        description="Transport protocol: 'tcp' (HTTP/2) or 'quic' (HTTP/3)",
        alias="APP_TRANSPORT",
    )

    # Server binding configuration
    server_host: str = Field(
        default="127.0.0.1",
        description="Server bind address",
        alias="SERVER_HOST",
    )

    server_port: int = Field(
        default=50051,
        description="Server port number",
        alias="SERVER_PORT",
    )

    # TLS certificate and key paths
    tls_cert_path: Path = Field(
        default=Path("./certs/localhost.pem"),
        description="Path to TLS 1.3 certificate file",
        alias="TLS_CERT_PATH",
    )

    tls_key_path: Path = Field(
        default=Path("./certs/localhost_key.pem"),
        description="Path to TLS 1.3 private key file",
        alias="TLS_KEY_PATH",
    )

    # Logging configuration
    log_dir: Path = Field(
        default=Path("./logs"),
        description="Directory for log files",
        alias="LOG_DIR",
    )

    log_level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = Field(
        default="INFO",
        description="Logging verbosity level",
        alias="LOG_LEVEL",
    )

    payment_failure_probability: float = Field(
        default=0.10,
        description="Probability of a simulated transient processing failure, between 0.0 and 1.0",
        alias="PAYMENT_FAILURE_PROBABILITY",
    )

    class Config:
        """Pydantic configuration."""

        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @field_validator("server_port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v

    @field_validator("app_transport")
    @classmethod
    def validate_transport(cls, v: str) -> str:
        """Validate transport is 'tcp' or 'quic'."""
        if v.lower() not in ("tcp", "quic"):
            raise ValueError(f"Transport must be 'tcp' or 'quic', got '{v}'")
        return v.lower()

    @field_validator("payment_failure_probability")
    @classmethod
    def validate_failure_probability(cls, v: float) -> float:
        """Validate the failure probability is between 0.0 and 1.0."""
        if not 0.0 <= v <= 1.0:
            raise ValueError(
                f"Payment failure probability must be between 0.0 and 1.0, got '{v}'"
            )
        return v

    def get_server_url(self) -> str:
        """Return server URL for display purposes."""
        return f"{self.server_host}:{self.server_port}"


def load_config() -> Config:
    """
    Load and validate application configuration from environment.

    Returns:
        Config: Validated configuration object.

    Raises:
        ValueError: If configuration is invalid.
    """
    return Config()
