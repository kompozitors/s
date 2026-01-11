from __future__ import annotations

import html
import json
import re
import time
from logging import getLogger
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from cardinal import Cardinal

from os.path import exists

from telebot.types import Message
from tg_bot import static_keyboards as skb


NAME = "Foreign Lots Cache Plugin"
VERSION = "0.1.1"
DESCRIPTION = "Плагин для выгрузки лотов с чужих профилей в JSON файл."
CREDITS = "@woopertail"
UUID = "8a47950f-0ebc-4c0a-bb4d-d4c2dc3fcfe6"
SETTINGS_PAGE = False


logger = getLogger("FPC.foreign_lots_plugin")
RUNNING = False

CBT_CACHE_FOREIGN_LOTS = "foreign_lots_plugin.cache"
"""
Callback для активации режима ожидания ввода профиля для выгрузки лотов.

User-state: ожидается ID профиля / ссылка и опциональный фильтр категории.
"""


SETTINGS_PATH = "storage/plugins/foreign_lots_settings.json"
settings = {
    "last_category": ""
}


def save_settings():
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        f.write(json.dumps(settings, indent=4, ensure_ascii=False))


def load_settings():
    if not exists(SETTINGS_PATH):
        return
    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        settings.update(json.loads(f.read()))


def normalize(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def category_values(lot_data: dict) -> Iterable[str]:
    values = []
    for key in ("category", "subcategory", "game", "section"):
        value = normalize(lot_data.get(key))
        if value:
            values.append(value)
    return values


def lot_matches_category(lot_data: dict, category_filter: str) -> bool:
    if not category_filter:
        return True
    target = normalize(category_filter)
    for value in category_values(lot_data):
        if target == value or target in value:
            return True
    return False


def parse_request(text: str) -> tuple[int | None, str]:
    parts = [part.strip() for part in text.split("|", 1)]
    profile_part = parts[0]
    category_filter = parts[1] if len(parts) > 1 else ""
    match = re.search(r"\d+", profile_part)
    if not match:
        return None, category_filter
    return int(match.group(0)), category_filter


def init_commands(cardinal: Cardinal):
    if not cardinal.telegram:
        return
    tg = cardinal.telegram
    bot = cardinal.telegram.bot

    def fetch_url(url: str) -> str:
        attempts = 3
        while attempts:
            try:
                response = cardinal.account.session.get(url, timeout=15)
                response.raise_for_status()
                response.encoding = response.apparent_encoding or "utf-8"
                return response.text
            except Exception:
                logger.error(f"[FOREIGN LOTS] Не удалось получить данные по URL {url}.")
                logger.debug("TRACEBACK", exc_info=True)
                time.sleep(1)
                attempts -= 1
        raise Exception

    def extract_meta(html_text: str, name: str) -> str:
        pattern = re.compile(
            rf'<meta[^>]+(?:property|name)="{re.escape(name)}"[^>]+content="([^"]+)"',
            re.IGNORECASE,
        )
        match = pattern.search(html_text)
        if not match:
            return ""
        return html.unescape(match.group(1)).strip()

    def extract_json_block(html_text: str, key: str) -> dict:
        index = html_text.find(f'"{key}":')
        if index == -1:
            return {}
        start = html_text.find("{", index)
        if start == -1:
            return {}
        depth = 0
        in_string = False
        escape = False
        for pos in range(start, len(html_text)):
            char = html_text[pos]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
            else:
                if char == '"':
                    in_string = True
                elif char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        block = html_text[start : pos + 1]
                        try:
                            return json.loads(block)
                        except Exception:
                            return {}
        return {}

    def parse_offer_page(offer_id: int, html_text: str) -> dict:
        data = {
            "offer_id": offer_id,
            "offer_url": f"https://funpay.com/lots/offer?id={offer_id}",
        }
        data.update(extract_json_block(html_text, "offer"))
        data.update(extract_json_block(html_text, "lot"))
        title = extract_meta(html_text, "og:title") or extract_meta(html_text, "title")
        description = extract_meta(html_text, "og:description") or extract_meta(html_text, "description")
        if title:
            data.setdefault("title", title)
        if description:
            data.setdefault("description", description)
        breadcrumbs = re.findall(r'class="breadcrumb-item"[^>]*>\\s*<a[^>]*>([^<]+)</a>', html_text)
        if breadcrumbs:
            data.setdefault("category", html.unescape(breadcrumbs[-1]).strip())
        price_match = re.search(r'data-sum="([0-9.,]+)"', html_text)
        if price_match:
            data.setdefault("price", price_match.group(1))
        return data

    def extract_offer_ids(html_text: str) -> set[int]:
        patterns = [
            r"/lots/offer\\?id=(\\d+)",
            r"offerId\"?\\s*:?\\s*(\\d+)",
            r"offer_id\"?\\s*:?\\s*(\\d+)",
            r"data-offer-id=\"(\\d+)\"",
        ]
        ids = set()
        for pattern in patterns:
            for match in re.findall(pattern, html_text):
                try:
                    ids.add(int(match))
                except ValueError:
                    continue
        return ids

    def get_profile_pages(profile_id: int) -> list[str]:
        base_url = f"https://funpay.com/users/{profile_id}/"
        html_text = fetch_url(base_url)
        pages = {base_url}
        page_matches = re.findall(rf"/users/{profile_id}/\\?page=(\\d+)", html_text)
        for page in page_matches:
            pages.add(f"{base_url}?page={page}")
        return sorted(pages), html_text

    def get_offer_ids(profile_id: int) -> list[int]:
        pages, first_html = get_profile_pages(profile_id)
        ids = set()
        ids.update(extract_offer_ids(first_html))
        for page_url in pages:
            if page_url.endswith("/"):
                continue
            html_text = fetch_url(page_url)
            ids.update(extract_offer_ids(html_text))
        return sorted(ids)

    def get_lots_info(tg_msg: Message, profile_id: int, category_filter: str) -> list[dict]:
        result = []
        offer_ids = get_offer_ids(profile_id)
        if not offer_ids:
            bot.send_message(tg_msg.chat.id, "❌ Не удалось найти лоты у профиля.")
            return result
        for offer_id in offer_ids:
            attempts = 3
            while attempts:
                try:
                    html_text = fetch_url(f"https://funpay.com/lots/offer?id={offer_id}")
                    lot_data = parse_offer_page(offer_id, html_text)
                    if not lot_matches_category(lot_data, category_filter):
                        break
                    result.append(lot_data)
                    logger.info(f"[FOREIGN LOTS] Получил данные о лоте {offer_id}.")
                    break
                except Exception:
                    logger.error(f"[FOREIGN LOTS] Не удалось получить данные о лоте {offer_id}.")
                    logger.debug("TRACEBACK", exc_info=True)
                    time.sleep(2)
                    attempts -= 1
            else:
                bot.send_message(
                    tg_msg.chat.id,
                    "❌ Не удалось получить данные о "
                    f"<a href=\"https://funpay.com/lots/offer?id={offer_id}\">лоте {offer_id}</a>. "
                    "Пропускаю.",
                )
                time.sleep(1)
                continue
            time.sleep(0.5)
        return result

    def act_cache_foreign_lots(m: Message):
        if RUNNING:
            bot.send_message(
                m.chat.id,
                "❌ Процесс выгрузки уже начался! Дождитесь конца текущего процесса.",
            )
            return
        hint = settings.get("last_category")
        hint_text = f" (последний фильтр: {hint})" if hint else ""
        result = bot.send_message(
            m.chat.id,
            "Отправьте ID профиля или ссылку на профиль. "
            "Опционально укажите фильтр категории через |, например: "
            "123456 | minecraft."
            f"{hint_text}",
            reply_markup=skb.CLEAR_STATE_BTN(),
        )
        tg.set_state(m.chat.id, result.id, m.from_user.id, CBT_CACHE_FOREIGN_LOTS)

    def cache_foreign_lots(m: Message):
        tg.clear_state(m.chat.id, m.from_user.id, True)
        profile_id, category_filter = parse_request(m.text)
        if not profile_id:
            bot.send_message(m.chat.id, "❌ Не удалось распознать ID профиля.")
            return

        global RUNNING
        RUNNING = True
        try:
            settings["last_category"] = category_filter
            save_settings()

            bot.send_message(
                m.chat.id,
                "Получаю данные о лотах (это может занять кое-какое время (1 лот/сек))...",
            )
            lots = get_lots_info(m, profile_id, category_filter)

            result = []
            for lot_data in lots:
                result.append(lot_data)

            file_name = f"foreign_lots_{profile_id}.json"
            file_path = f"storage/cache/{file_name}"
            bot.send_message(m.chat.id, "Сохраняю данные о лотах в файл и отправляю сюда...")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json.dumps(result, indent=4, ensure_ascii=False))
            with open(file_path, "rb") as f:
                bot.send_document(m.chat.id, f)
            RUNNING = False
            bot.send_message(m.chat.id, "✅ Выгрузка лотов завершена!")
        except Exception:
            RUNNING = False
            logger.error("[FOREIGN LOTS] Не удалось выгрузить лоты.")
            logger.debug("TRACEBACK", exc_info=True)
            bot.send_message(m.chat.id, "❌ Не удалось выгрузить лоты.")
            return

    cardinal.add_telegram_commands(
        UUID,
        [
            ("cache_foreign_lots", "выгружает лоты чужого профиля в JSON", True),
        ],
    )

    tg.msg_handler(act_cache_foreign_lots, commands=["cache_foreign_lots"])
    tg.msg_handler(
        cache_foreign_lots,
        func=lambda m: tg.check_state(m.chat.id, m.from_user.id, CBT_CACHE_FOREIGN_LOTS),
    )

    load_settings()
    logger.info("[FOREIGN LOTS] Настройки выгрузки лотов загружены.")


BIND_TO_PRE_INIT = [init_commands]
BIND_TO_DELETE = None
