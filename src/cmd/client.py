"""Client entry point for the dual-stack payment POC."""

import asyncio

from src.config import load_config
from src.generated import payment_pb2
from src.logger import create_logger
from src.transport.factory import create_client


async def run_client() -> payment_pb2.PaymentResponse:
    """Execute unary and streaming payment RPCs over the configured transport."""
    config = load_config()
    logger = create_logger("client", config)
    client = create_client(config, logger)
    request = payment_pb2.PaymentRequest(
        amount_cents=10000,
        merchant_id="demo_merchant",
        customer_id="demo_customer",
        description="Dual-stack transport demonstration",
        currency="USD",
    )

    try:
        await client.connect()
        response = await client.process_payment(request)
        print(
            f"Payment {response.message}: "
            f"transaction_id={response.transaction_id}"
        )

        status_request = payment_pb2.StreamStatusRequest(
            transaction_id=response.transaction_id
        )
        async for update in client.stream_payment_status(status_request):
            status_name = payment_pb2.PaymentStatusUpdate.StatusCode.Name(
                update.status
            )
            print(f"Status: {status_name} ({update.progress_percent}%)")
        return response
    finally:
        await client.close()


def main() -> None:
    """Run the client entry point."""
    asyncio.run(run_client())


if __name__ == "__main__":
    main()
