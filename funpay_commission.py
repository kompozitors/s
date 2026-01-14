"""Utility to calculate buyer-facing price with FunPay-style commission."""
from __future__ import annotations

import argparse
from decimal import Decimal, ROUND_HALF_UP


def _to_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except Exception as exc:  # pragma: no cover - defensive
        raise argparse.ArgumentTypeError(f"Invalid number: {value}") from exc


def calculate_buyer_price(
    seller_price: Decimal,
    commission_percent: Decimal,
    *,
    mode: str,
    precision: int = 2,
) -> Decimal:
    """Calculate buyer-facing price.

    Args:
        seller_price: Price you want to set/receive.
        commission_percent: Commission in percent (e.g., 17.85).
        mode:
            - "add": buyer price = seller_price * (1 + commission%).
            - "gross": buyer price = seller_price / (1 - commission%).
        precision: Decimal places to round to.
    """
    if seller_price <= 0:
        raise ValueError("seller_price must be positive")
    if commission_percent < 0 or commission_percent >= 100:
        raise ValueError("commission_percent must be in [0, 100)")

    rate = commission_percent / Decimal("100")
    if mode == "add":
        buyer_price = seller_price * (Decimal("1") + rate)
    elif mode == "gross":
        buyer_price = seller_price / (Decimal("1") - rate)
    else:
        raise ValueError("mode must be 'add' or 'gross'")

    quant = Decimal("1").scaleb(-precision)
    return buyer_price.quantize(quant, rounding=ROUND_HALF_UP)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate buyer-facing price given a seller price and commission."
        )
    )
    parser.add_argument(
        "price",
        type=_to_decimal,
        help="Seller price (number, e.g. 1300)",
    )
    parser.add_argument(
        "commission",
        type=_to_decimal,
        help="Commission percent (e.g. 17.85)",
    )
    parser.add_argument(
        "--mode",
        choices=["add", "gross"],
        default="add",
        help="add: add commission on top, gross: seller gets price after fee",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=2,
        help="Decimal places for rounding (default: 2)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    result = calculate_buyer_price(
        args.price,
        args.commission,
        mode=args.mode,
        precision=args.precision,
    )
    print(result)


if __name__ == "__main__":
    main()
