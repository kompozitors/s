"""Fetch FunPay category commissions and write them to a text file."""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, List
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


PROMO_GAME_ITEM_RE = re.compile(
    r'<div class="promo-game-item".*?>.*?</div>\s*</div>',
    re.DOTALL,
)
GAME_TITLE_RE = re.compile(
    r'<div class="game-title">\s*<a[^>]*>(?P<title>.*?)</a>',
    re.DOTALL,
)
LIST_INLINE_RE = re.compile(
    r'<ul class="list-inline"[^>]*data-id="(?P<game_id>\d+)"[^>]*>(?P<body>.*?)</ul>',
    re.DOTALL,
)
NODE_LINK_RE = re.compile(
    r'<a[^>]+href="[^"]+/(lots|chips)/(?P<node_id>\d+)"[^>]*>',
    re.DOTALL,
)


def _quantize(value: Decimal, precision: int) -> Decimal:
    quant = Decimal("1").scaleb(-precision)
    return value.quantize(quant, rounding=ROUND_HALF_UP)


def fetch_html(url: str, *, timeout: float = 15.0) -> str:
    req = request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; funpay-komka/1.0)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8")


def parse_categories(html: str) -> List[Category]:
    categories: List[Category] = []
    for block in PROMO_GAME_ITEM_RE.findall(html):
        title_match = GAME_TITLE_RE.search(block)
        if not title_match:
            continue
        title = unescape(title_match.group("title").strip())
        if not title:
            continue
        for list_match in LIST_INLINE_RE.finditer(block):
            body = list_match.group("body")
            for node_match in NODE_LINK_RE.finditer(body):
                node_id = int(node_match.group("node_id"))
                categories.append(Category(node_id=node_id, game_name=title))
    return categories


def fetch_categories() -> List[Category]:
    html = fetch_html("https://funpay.com")
    return parse_categories(html)


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
