"""
Protocol-agnostic payment service business logic.

Implements payment processing and status streaming without coupling to
any specific transport protocol (TCP or QUIC). All logic is transport-neutral.
"""

import asyncio
import hashlib
import random
import time
from typing import Any, AsyncGenerator

from src.generated import payment_pb2


class PaymentService:
    """
    Business logic for payment processing.

    Handles unary RPC (ProcessPayment) and streaming RPC (StreamPaymentStatus).
    Completely decoupled from transport layer (TCP vs QUIC).
    """

    def __init__(
        self,
        failure_probability: float = 0.10,
        logger: Any | None = None,
        transport: str | None = None,
    ):
        """Initialize the payment service."""
        if not 0.0 <= failure_probability <= 1.0:
            raise ValueError(
                "Failure probability must be between 0.0 and 1.0 inclusive."
            )
        self.failure_probability = failure_probability
        self.logger = logger
        self.transport = transport or "SERVICE"
        # In-memory store for demonstration (transaction_id -> status)
        self._transactions: dict[str, dict] = {}

    async def process_payment(
        self, request: payment_pb2.PaymentRequest
    ) -> payment_pb2.PaymentResponse:
        """
        Process a single payment request (unary RPC).

        Args:
            request: PaymentRequest containing transaction details.

        Returns:
            PaymentResponse with authorization status and transaction ID.
        """
        request_size = request.ByteSize()
        self._log_event(
            "Payment request received",
            rpc_method="ProcessPayment",
            status_code="INFO",
            payload_size_bytes=request_size,
        )

        # Validate request
        if request.amount_cents <= 0:
            self._log_event(
                "Payment request rejected: invalid amount",
                rpc_method="ProcessPayment",
                status_code="DECLINED",
                payload_size_bytes=request_size,
            )
            return payment_pb2.PaymentResponse(
                status=payment_pb2.PaymentResponse.Status.DECLINED,
                transaction_id="",
                message="Invalid amount: must be positive",
                timestamp_ms=int(time.time() * 1000),
            )

        if not request.merchant_id:
            self._log_event(
                "Payment request rejected: missing merchant_id",
                rpc_method="ProcessPayment",
                status_code="ERROR",
                payload_size_bytes=request_size,
            )
            return payment_pb2.PaymentResponse(
                status=payment_pb2.PaymentResponse.Status.ERROR,
                transaction_id="",
                message="Merchant ID is required",
                timestamp_ms=int(time.time() * 1000),
            )

        if not request.customer_id:
            self._log_event(
                "Payment request rejected: missing customer_id",
                rpc_method="ProcessPayment",
                status_code="ERROR",
                payload_size_bytes=request_size,
            )
            return payment_pb2.PaymentResponse(
                status=payment_pb2.PaymentResponse.Status.ERROR,
                transaction_id="",
                message="Customer ID is required",
                timestamp_ms=int(time.time() * 1000),
            )

        # Generate deterministic transaction ID based on request content
        transaction_id = self._generate_transaction_id(request)

        # Inject a transient processing failure according to the configured probability.
        if self._should_inject_failure():
            self._transactions[transaction_id] = {
                "merchant_id": request.merchant_id,
                "customer_id": request.customer_id,
                "amount_cents": request.amount_cents,
                "currency": request.currency,
                "status": "FAILED",
                "created_at": time.time(),
                "error": "Temporary processing failure",
            }
            self._log_event(
                "Payment failure injected by probability gate",
                rpc_method="ProcessPayment",
                status_code="ERROR",
                payload_size_bytes=request_size,
            )
            return payment_pb2.PaymentResponse(
                status=payment_pb2.PaymentResponse.Status.ERROR,
                transaction_id=transaction_id,
                message="Temporary processing failure. Please retry.",
                timestamp_ms=int(time.time() * 1000),
            )

        # Simulate payment processing logic (deterministic for testing)
        is_approved = self._determine_approval(request, transaction_id)

        # Store transaction for status streaming
        self._transactions[transaction_id] = {
            "merchant_id": request.merchant_id,
            "customer_id": request.customer_id,
            "amount_cents": request.amount_cents,
            "currency": request.currency,
            "status": "AUTHORIZED" if is_approved else "DECLINED",
            "created_at": time.time(),
        }

        response = payment_pb2.PaymentResponse(
            status=payment_pb2.PaymentResponse.Status.APPROVED
            if is_approved
            else payment_pb2.PaymentResponse.Status.DECLINED,
            transaction_id=transaction_id,
            message="Payment approved" if is_approved else "Payment declined",
            timestamp_ms=int(time.time() * 1000),
        )
        self._log_event(
            "Payment processed successfully",
            rpc_method="ProcessPayment",
            status_code="OK",
            payload_size_bytes=request_size,
        )
        return response

    async def stream_payment_status(
        self, request: payment_pb2.StreamStatusRequest
    ) -> AsyncGenerator[payment_pb2.PaymentStatusUpdate, None]:
        """
        Stream payment status updates (server-streaming RPC).

        Args:
            request: StreamStatusRequest with transaction_id to monitor.

        Yields:
            PaymentStatusUpdate messages with progress updates.
        """
        transaction_id = request.transaction_id

        # Verify transaction exists
        if transaction_id not in self._transactions:
            yield payment_pb2.PaymentStatusUpdate(
                status=payment_pb2.PaymentStatusUpdate.StatusCode.FAILED,
                progress_percent=0,
                description=f"Transaction {transaction_id} not found",
                timestamp_ms=int(time.time() * 1000),
            )
            return

        transaction = self._transactions[transaction_id]

        # Simulate payment lifecycle with status transitions
        if transaction["status"] == "AUTHORIZED":
            status_sequence = [
                (payment_pb2.PaymentStatusUpdate.StatusCode.PENDING, 10, "Payment pending"),
                (
                    payment_pb2.PaymentStatusUpdate.StatusCode.PROCESSING,
                    30,
                    "Processing payment",
                ),
                (
                    payment_pb2.PaymentStatusUpdate.StatusCode.AUTHORIZED,
                    60,
                    "Payment authorized",
                ),
                (payment_pb2.PaymentStatusUpdate.StatusCode.CAPTURED, 80, "Funds captured"),
                (payment_pb2.PaymentStatusUpdate.StatusCode.SETTLED, 100, "Payment settled"),
            ]
        else:
            status_sequence = [
                (payment_pb2.PaymentStatusUpdate.StatusCode.PENDING, 10, "Payment pending"),
                (
                    payment_pb2.PaymentStatusUpdate.StatusCode.PROCESSING,
                    50,
                    "Processing payment",
                ),
                (
                    payment_pb2.PaymentStatusUpdate.StatusCode.FAILED,
                    100,
                    "Payment declined",
                ),
            ]

        # Stream status updates
        for status_code, progress, description in status_sequence:
            yield payment_pb2.PaymentStatusUpdate(
                status=status_code,
                progress_percent=progress,
                description=description,
                timestamp_ms=int(time.time() * 1000),
            )

            if progress < 100:
                await asyncio.sleep(0.2)

    def _log_event(
        self,
        message: str,
        rpc_method: str,
        status_code: str,
        payload_size_bytes: int,
        duration_ms: float = 0.0,
    ) -> None:
        """Emit a service-level log line when a logger is configured."""
        if self.logger is None:
            return
        self.logger.info(
            message,
            transport=self.transport,
            rpc_method=rpc_method,
            duration_ms=duration_ms,
            status_code=status_code,
            payload_size_bytes=payload_size_bytes,
        )

    def _should_inject_failure(self) -> bool:
        """Return True for requests selected by the configured failure probability."""
        return random.random() < self.failure_probability

    def _generate_transaction_id(
        self, request: payment_pb2.PaymentRequest
    ) -> str:
        """
        Generate a deterministic transaction ID from request.

        Args:
            request: Payment request.

        Returns:
            Transaction ID string.
        """
        # Deterministic ID based only on request content
        content = (
            f"{request.merchant_id}:{request.customer_id}:"
            f"{request.amount_cents}:{request.currency}:{request.description}"
        )
        hash_digest = hashlib.sha256(content.encode()).hexdigest()[:16]
        # Use UUID-style formatting for readability
        return f"txn_{hash_digest}"

    def _determine_approval(
        self, request: payment_pb2.PaymentRequest, transaction_id: str
    ) -> bool:
        """
        Deterministically determine payment approval.

        Args:
            request: Payment request.
            transaction_id: Generated transaction ID.

        Returns:
            True if payment should be approved, False otherwise.
        """
        # Deterministic approval logic based on merchant and amount
        # For testing: approve if amount is < 1,000,000 cents ($10,000)
        if request.amount_cents >= 1_000_000:
            return False

        # Approve if customer ID doesn't contain "decline"
        if "decline" in request.customer_id.lower():
            return False

        # Approve if merchant ID doesn't contain "fail"
        if "fail" in request.merchant_id.lower():
            return False

        # Default: approve
        return True

    def get_transaction(self, transaction_id: str) -> dict | None:
        """
        Retrieve transaction details.

        Args:
            transaction_id: Transaction ID to look up.

        Returns:
            Transaction details dict or None if not found.
        """
        return self._transactions.get(transaction_id)

    def list_transactions(self) -> list[dict]:
        """
        List all transactions.

        Returns:
            List of transaction details.
        """
        return list(self._transactions.values())
