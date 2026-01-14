"""Fetch FunPay category commissions and write them to a text file."""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib import parse, request


CURRENCY_SYMBOLS = {
    "RUB": "₽",
    "USD": "$",
    "EUR": "€",
}


@dataclass(frozen=True)
class Category:
    node_id: int
    game_name: str


class FunPayCategoryParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_game_item = False
        self._in_game_title = False
        self._current_game_name: Optional[str] = None
        self.categories: List[Category] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        attr_dict = dict(attrs)
        class_value = attr_dict.get("class") or ""
        classes = set(class_value.split())

        if tag == "div" and "promo-game-item" in classes:
            self._in_game_item = True
            self._current_game_name = None

        if self._in_game_item and tag == "div" and "game-title" in classes:
            self._in_game_title = True

        if self._in_game_item and tag == "a":
            href = attr_dict.get("href") or ""
            match = re.search(r"/(lots|chips)/(\d+)", href)
            if match and self._current_game_name:
                node_id = int(match.group(2))
                self.categories.append(
                    Category(node_id=node_id, game_name=self._current_game_name)
                )

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self._in_game_title:
            self._in_game_title = False
        if tag == "div" and self._in_game_item and not self._in_game_title:
            self._in_game_item = False

    def handle_data(self, data: str) -> None:
        if self._in_game_title:
            cleaned = data.strip()
            if cleaned:
                self._current_game_name = cleaned


def _quantize(value: Decimal, precision: int) -> Decimal:
    quant = Decimal("1").scaleb(-precision)
    return value.quantize(quant, rounding=ROUND_HALF_UP)


def fetch_html(url: str, *, timeout: float = 15.0) -> str:
    with request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def fetch_categories() -> List[Category]:
    html = fetch_html("https://funpay.com")
    parser = FunPayCategoryParser()
    parser.feed(html)
    return parser.categories


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
    commission = (min_price - base_price) / min_price * Decimal("100")
    return _quantize(commission, 2)


def dedupe_categories(categories: Iterable[Category]) -> List[Category]:
    seen: Dict[int, Category] = {}
    for category in categories:
        seen.setdefault(category.node_id, category)
    return list(seen.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch FunPay commissions for all categories."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("funpay_commissions.txt"),
        help="Output text file path (default: funpay_commissions.txt)",
    )
    parser.add_argument(
        "--currency",
        choices=sorted(CURRENCY_SYMBOLS.keys()),
        default="RUB",
        help="Currency used for commission lookup (default: RUB)",
    )
    parser.add_argument(
        "--base-price",
        type=Decimal,
        default=Decimal("100"),
        help="Base price to send to /lots/calc (default: 100)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    categories = dedupe_categories(fetch_categories())
    lines: List[str] = []
    for category in categories:
        commission = fetch_commission_percent(
            category.node_id,
            currency=args.currency,
            base_price=args.base_price,
        )
        lines.append(f"{category.node_id} | {category.game_name} | {commission}%")

    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(lines)} lines to {args.output}")


if __name__ == "__main__":
    main()
