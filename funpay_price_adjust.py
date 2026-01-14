"""Adjust FunPay lot prices using category commission percentages."""
from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib import parse, request


CURRENCY_SYMBOLS = {
    "RUB": "₽",
    "USD": "$",
    "EUR": "€",
}


def _to_decimal(value: str) -> Decimal:
    try:
        return Decimal(value)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid number: {value}") from exc


def _quantize(value: Decimal, precision: int) -> Decimal:
    quant = Decimal("1").scaleb(-precision)
    return value.quantize(quant, rounding=ROUND_HALF_UP)


def buyer_price_from_seller(
    seller_price: Decimal,
    commission_percent: Decimal,
    *,
    precision: int = 2,
) -> Decimal:
    if seller_price <= 0:
        raise ValueError("seller_price must be positive")
    if commission_percent < 0 or commission_percent >= 100:
        raise ValueError("commission_percent must be in [0, 100)")

    rate = commission_percent / Decimal("100")
    buyer_price = seller_price / (Decimal("1") - rate)
    return _quantize(buyer_price, precision)


def seller_price_from_buyer(
    buyer_price: Decimal,
    commission_percent: Decimal,
    *,
    precision: int = 3,
) -> Decimal:
    if buyer_price <= 0:
        raise ValueError("buyer_price must be positive")
    if commission_percent < 0 or commission_percent >= 100:
        raise ValueError("commission_percent must be in [0, 100)")

    rate = commission_percent / Decimal("100")
    seller_price = buyer_price * (Decimal("1") - rate)
    return _quantize(seller_price, precision)


def _parse_price(value: str) -> Decimal:
    normalized = value.replace(" ", "").replace(",", ".")
    return Decimal(normalized)


def fetch_commission_percent(
    node_id: int,
    *,
    currency: str,
    base_price: Decimal,
    timeout: float = 10.0,
) -> Decimal:
    form = parse.urlencode({"nodeId": str(node_id), "price": str(base_price)}).encode(
        "utf-8"
    )
    req = request.Request(
        "https://funpay.com/lots/calc",
        data=form,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    methods = payload.get("methods") or []
    symbol = CURRENCY_SYMBOLS.get(currency)
    if not symbol:
        raise ValueError(f"Unsupported currency: {currency}")

    prices: List[Decimal] = []
    for method in methods:
        if method.get("unit") != symbol:
            continue
        price_string = str(method.get("price", "0"))
        prices.append(_parse_price(price_string))

    if not prices:
        raise ValueError(f"No methods for currency {currency} in node {node_id}")

    min_price = min(prices)
    return (min_price - base_price) / min_price * Decimal("100")


def load_commission_map(path: Optional[Path]) -> Dict[str, Decimal]:
    if not path:
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): _to_decimal(str(value)) for key, value in data.items()}


def _load_lots(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("lots.json should contain a list of lots")
    return data


def _dump_lots(path: Path, lots: List[Dict[str, Any]]) -> None:
    path.write_text(
        json.dumps(lots, indent=4, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _iter_node_ids(lots: Iterable[Dict[str, Any]]) -> List[int]:
    node_ids: List[int] = []
    for lot in lots:
        node = lot.get("node_id")
        if node is None:
            continue
        try:
            node_ids.append(int(node))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid node_id value: {node}") from exc
    return sorted(set(node_ids))


def _commission_base_price(currency: str) -> Decimal:
    return Decimal("100000") if currency == "RUB" else Decimal("1000")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Adjust lots.json prices using FunPay commission."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("lots.json"),
        help="Input lots.json path (default: lots.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path (default: overwrite input)",
    )
    parser.add_argument(
        "--mode",
        choices=["seller-to-buyer", "buyer-to-seller"],
        default="seller-to-buyer",
        help="Direction of recalculation",
    )
    parser.add_argument(
        "--price-field",
        default="price",
        help="Lot field to update (default: price)",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=None,
        help="Override decimal rounding precision",
    )
    parser.add_argument(
        "--currency",
        choices=sorted(CURRENCY_SYMBOLS.keys()),
        default="RUB",
        help="Currency used for commission lookup (default: RUB)",
    )
    parser.add_argument(
        "--commission-map",
        type=Path,
        default=None,
        help="JSON file with node_id -> commission_percent mapping",
    )
    parser.add_argument(
        "--fetch-commission",
        action="store_true",
        help="Fetch commission percentages from funpay.com for node_id values",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip lots without known commission instead of failing",
    )
    return parser


def build_commission_map(
    lots: List[Dict[str, Any]],
    *,
    currency: str,
    existing: Dict[str, Decimal],
    fetch: bool,
) -> Dict[str, Decimal]:
    commissions = dict(existing)
    if not fetch:
        return commissions
    base_price = _commission_base_price(currency)
    for node_id in _iter_node_ids(lots):
        key = str(node_id)
        if key in commissions:
            continue
        commissions[key] = fetch_commission_percent(
            node_id, currency=currency, base_price=base_price
        )
    return commissions


def adjust_prices(
    lots: List[Dict[str, Any]],
    *,
    commission_map: Dict[str, Decimal],
    mode: str,
    price_field: str,
    precision: Optional[int],
    skip_missing: bool,
) -> Tuple[int, List[str]]:
    updated = 0
    missing: List[str] = []
    for lot in lots:
        node_id = lot.get("node_id")
        if node_id is None:
            missing.append("<missing node_id>")
            if skip_missing:
                continue
            raise ValueError("Lot is missing node_id")

        commission = commission_map.get(str(node_id))
        if commission is None:
            missing.append(str(node_id))
            if skip_missing:
                continue
            raise ValueError(f"Missing commission for node_id {node_id}")

        raw_price = lot.get(price_field)
        if raw_price is None:
            missing.append(f"{node_id}:{price_field}")
            if skip_missing:
                continue
            raise ValueError(f"Lot is missing {price_field} for node_id {node_id}")

        price_decimal = _parse_price(str(raw_price))
        if mode == "seller-to-buyer":
            round_to = 2 if precision is None else precision
            new_price = buyer_price_from_seller(
                price_decimal, commission, precision=round_to
            )
        else:
            round_to = 3 if precision is None else precision
            new_price = seller_price_from_buyer(
                price_decimal, commission, precision=round_to
            )

        lot[price_field] = format(new_price, "f")
        updated += 1

    return updated, missing


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    lots = _load_lots(args.input)
    commission_map = load_commission_map(args.commission_map)
    commission_map = build_commission_map(
        lots,
        currency=args.currency,
        existing=commission_map,
        fetch=args.fetch_commission,
    )

    updated, missing = adjust_prices(
        lots,
        commission_map=commission_map,
        mode=args.mode,
        price_field=args.price_field,
        precision=args.precision,
        skip_missing=args.skip_missing,
    )

    output = args.output or args.input
    _dump_lots(output, lots)

    print(f"Updated lots: {updated}")
    if missing:
        print("Missing entries:", ", ".join(missing))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
