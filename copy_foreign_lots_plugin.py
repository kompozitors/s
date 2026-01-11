from __future__ import annotations

import json
import re
import time
from logging import getLogger
from typing import TYPE_CHECKING, Iterable

import FunPayAPI.types

if TYPE_CHECKING:
    from cardinal import Cardinal

from os.path import exists

from telebot.types import Message
from tg_bot import static_keyboards as skb


NAME = "Foreign Lots Cache Plugin"
VERSION = "0.1.0"
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


def category_values(lot: FunPayAPI.types.Lot) -> Iterable[str]:
    sub = getattr(lot, "subcategory", None)
    if not sub:
        return []
    values = []
    for attr in ("name", "title", "full_name", "short_name"):
        value = normalize(getattr(sub, attr, None))
        if value:
            values.append(value)
    for attr in ("category_id", "id"):
        value = normalize(getattr(sub, attr, None))
        if value:
            values.append(value)
    cat = getattr(sub, "category", None)
    if cat:
        for attr in ("name", "title", "full_name", "short_name"):
            value = normalize(getattr(cat, attr, None))
            if value:
                values.append(value)
        value = normalize(getattr(cat, "id", None))
        if value:
            values.append(value)
    return values


def lot_matches_category(lot: FunPayAPI.types.Lot, category_filter: str) -> bool:
    if not category_filter:
        return True
    target = normalize(category_filter)
    for value in category_values(lot):
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

    def get_profile(tg_msg: Message, profile_id: int) -> FunPayAPI.types.UserProfile:
        attempts = 3
        while attempts:
            try:
                profile = cardinal.account.get_user(profile_id)
                return profile
            except Exception:
                logger.error("[FOREIGN LOTS] Не удалось получить данные о профиле.")
                logger.debug("TRACEBACK", exc_info=True)
                time.sleep(1)
                attempts -= 1
        bot.send_message(tg_msg.chat.id, "❌ Не удалось получить данные профиля.")
        raise Exception

    def get_lots_info(
        tg_msg: Message,
        profile: FunPayAPI.types.UserProfile,
        category_filter: str,
    ) -> list[FunPayAPI.types.LotFields]:
        result = []
        for lot in profile.get_lots():
            if lot.subcategory.type == FunPayAPI.types.SubCategoryTypes.CURRENCY:
                continue
            if not lot_matches_category(lot, category_filter):
                continue
            attempts = 3
            while attempts:
                try:
                    lot_fields = cardinal.account.get_lot_fields(lot.id)
                    result.append(lot_fields)
                    logger.info(f"[FOREIGN LOTS] Получил данные о лоте {lot.id}.")
                    break
                except Exception:
                    logger.error(f"[FOREIGN LOTS] Не удалось получить данные о лоте {lot.id}.")
                    logger.debug("TRACEBACK", exc_info=True)
                    time.sleep(2)
                    attempts -= 1
            else:
                bot.send_message(
                    tg_msg.chat.id,
                    "❌ Не удалось получить данные о "
                    f"<a href=\"https://funpay.com/lots/offer?id={lot.id}\">лоте {lot.id}</a>. "
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

            bot.send_message(m.chat.id, "Получаю данные о профиле...")
            profile = get_profile(m, profile_id)

            bot.send_message(
                m.chat.id,
                "Получаю данные о лотах (это может занять кое-какое время (1 лот/сек))...",
            )
            lots = get_lots_info(m, profile, category_filter)

            result = []
            for lot_fields in lots:
                fields = dict(lot_fields.fields)
                fields.pop("csrf_token", None)
                fields.pop("offer_id", None)
                result.append(fields)

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
