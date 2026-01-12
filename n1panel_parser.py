#!/usr/bin/env python3
"""Fetch services from n1panel API and export to JSON or text."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

API_URL = "https://n1panel.com/api/v2"


@dataclass
class Service:
    service: int
    name: str
    type: str
    category: str
    price: str
    min: str
    max: str
    refill: bool
    cancel: bool
    description: str

    @classmethod
    def from_api(cls, payload: dict[str, Any]) -> "Service":
        return cls(
            service=int(payload.get("service")),
            name=str(payload.get("name", "")),
            type=str(payload.get("type", "")),
            category=str(payload.get("category", "")),
            price=str(payload.get("rate", "")),
            min=str(payload.get("min", "")),
            max=str(payload.get("max", "")),
            refill=bool(payload.get("refill")),
            cancel=bool(payload.get("cancel")),
            description=str(payload.get("description", "")),
        )

    def to_text(self) -> str:
        return (
            f"Service ID: {self.service}\n"
            f"Name: {self.name}\n"
            f"Type: {self.type}\n"
            f"Category: {self.category}\n"
            f"Price: {self.price}\n"
            f"Min: {self.min}\n"
            f"Max: {self.max}\n"
            f"Refill: {self.refill}\n"
            f"Cancel: {self.cancel}\n"
            f"Description: {self.description}\n"
        )


def fetch_services(api_key: str) -> list[Service]:
    response = requests.post(
        API_URL,
        data={
            "key": api_key,
            "action": "services",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected response: {payload}")
    return [Service.from_api(item) for item in payload]


def write_json(services: list[Service], path: Path) -> None:
    data = [service.__dict__ for service in services]
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_txt(services: list[Service], path: Path) -> None:
    lines = []
    for service in services:
        lines.append(service.to_text())
        lines.append("-" * 40 + "\n")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch n1panel services (including prices and descriptions) and save them to JSON or text.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("N1PANEL_API_KEY"),
        help="API key from n1panel (or set N1PANEL_API_KEY)",
    )
    parser.add_argument(
        "--format",
        choices=("json", "txt"),
        default="json",
        help="Output format",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path (default: services.json or services.txt)",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if not args.api_key:
        raise SystemExit("Error: provide --api-key or set N1PANEL_API_KEY")
    services = fetch_services(args.api_key)
    output_path = Path(args.output or f"services.{args.format}")
    if args.format == "json":
        write_json(services, output_path)
    else:
        write_txt(services, output_path)
    print(f"Saved {len(services)} services to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
