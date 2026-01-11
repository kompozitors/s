from __future__ import annotations
from typing import TYPE_CHECKING, List, Dict, Optional, Tuple, Any, Set
import os
import re
import json
import time
import random
import string
import logging
import concurrent.futures
import threading
from threading import Lock, Event
from collections import defaultdict, deque
import signal

from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

if TYPE_CHECKING:
    from cardinal import Cardinal

from FunPayAPI.account import Account
from FunPayAPI.types import UserProfile, LotShortcut, LotPage

NAME = "Авто-Копирование"
VERSION = "4.0"
DESCRIPTION = "Копирует публичные лоты (RU+EN) с чужого профиля."
CREDITS = "@exfador"
UUID = "96b3d870-4bda-4025-9d46-d14a460ade30"
SETTINGS_PAGE = False

# Константы для оптимизации
MAX_WORKERS = 8  # Максимальное количество параллельных потоков
LOCALE_CACHE_TTL = 1200  # TTL для кеша локализаций в секундах (20 минут)
REQUEST_DELAY_MIN = 0.5  # Минимальная задержка между запросами в секундах
REQUEST_DELAY_MAX = 1.5  # Максимальная задержка между запросами в секундах
PROGRESS_UPDATE_INTERVAL = 1  # Интервал обновления статуса в секундах
BATCH_SIZE = 15  # Размер пакета лотов для одновременной обработки
MAX_CACHE_SIZE = 300  # Максимальный размер кеша
CACHE_CLEANUP_THRESHOLD = 200  # Порог для запуска очистки кеша

logger = logging.getLogger("FPC.auto_copy")
locale_cache = {}  # Кеш для хранения локализованных данных: {lot_id_locale: (data, timestamp)}
locale_cache_lock = Lock()  # Блокировка для безопасного доступа к кешу
cache_hit_stats = defaultdict(int)  # Статистика использования кеша: {locale: hit_count}
cache_miss_stats = defaultdict(int)  # Статистика промахов кеша: {locale: miss_count}
stats_lock = Lock()  # Блокировка для статистики
active_users_lock = Lock()  # Блокировка для активных пользователей

# Состояния для конечного автомата
STATE_WAIT_LINK = "AC_WAIT_LINK"
STATE_PROCESSING = "AC_PROCESSING"
user_data = {}  # Данные пользователя: {chat_id: {"step": state, "data": {}, "cancel_event": Event}}
active_tasks = {}  # Активные задачи: {chat_id: future}

# Кнопка отмены копирования
cancel_button = InlineKeyboardMarkup()
cancel_button.add(InlineKeyboardButton("🚫 Отменить копирование", callback_data="cancel_copy"))

def cleanup_cache(force=False):
    """Очищает старые записи из кеша, сохраняя наиболее часто используемые."""
    with locale_cache_lock:
        cache_size = len(locale_cache)
        if not force and cache_size <= CACHE_CLEANUP_THRESHOLD:
            return
        
        # Получаем текущее время и фильтруем записи по времени
        current_time = time.time()
        # Сортируем по времени доступа (от старых к новым)
        sorted_cache = sorted(
            [(k, v) for k, v in locale_cache.items()],
            key=lambda x: x[1][1]  # x[1][1] это timestamp
        )
        
        # Удаляем старые записи, оставляя только MAX_CACHE_SIZE - CACHE_CLEANUP_THRESHOLD
        to_remove = len(sorted_cache) - (MAX_CACHE_SIZE - CACHE_CLEANUP_THRESHOLD)
        to_remove = max(0, to_remove)  # Убедимся, что не отрицательное
        
        for i in range(min(to_remove, len(sorted_cache))):
            key = sorted_cache[i][0]
            locale_cache.pop(key, None)
        
        if to_remove > 0:
            logger.info(f"[Авто-Копирование] Очистка кеша: удалено {to_remove} записей")

def random_filename(username: str) -> str:
    """Возвращает имя файла вида {username}_{timestamp}_{rnd}.json"""
    t = int(time.time())
    r = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"{username}_{t}_{r}.json"

def extract_user_id(link: str) -> int | None:
    """Извлекает ID пользователя из ссылки на профиль FunPay."""
    pattern = re.compile(r"https?://funpay\.com/users/(\d+)/?")
    m = pattern.search(link)
    if m:
        return int(m.group(1))
    return None

def update_cache_stats(locale: str, cache_hit: bool):
    """Обновляет статистику использования кеша."""
    with stats_lock:
        if cache_hit:
            cache_hit_stats[locale] += 1
        else:
            cache_miss_stats[locale] += 1

def get_cached_locale_data(acc: Account, lot_id: int, locale: str) -> Tuple[Optional[LotPage], bool]:
    """
    Получает данные лота для указанной локали с использованием кеша.
    Возвращает (данные_лота, использован_кеш)
    """
    cache_key = f"{lot_id}_{locale}"
    current_time = time.time()
    
    with locale_cache_lock:
        # Проверка наличия данных в кеше и их актуальности
        if cache_key in locale_cache:
            cached_data, timestamp = locale_cache[cache_key]
            if current_time - timestamp < LOCALE_CACHE_TTL:
                update_cache_stats(locale, True)
                return cached_data, True
    
    # Установка локали и получение данных
    orig_locale = acc.locale
    acc.locale = locale
    
    try:
        lot_page = acc.get_lot_page(lot_id, locale=locale)
        
        # Сохранение данных в кеш
        with locale_cache_lock:
            locale_cache[cache_key] = (lot_page, current_time)
            
            # Проверяем, нужно ли очистить кеш
            if len(locale_cache) > MAX_CACHE_SIZE:
                cleanup_cache()
        
        update_cache_stats(locale, False)
        return lot_page, False
    except Exception as e:
        logger.warning(f"get_lot_page {locale} error for lot {lot_id}: {e}")
        update_cache_stats(locale, False)
        return None, False
    finally:
        # Восстановление исходной локали
        acc.locale = orig_locale

def build_json_for_lot(acc: Account, lot: LotShortcut, cancel_event=None) -> dict:
    """Собирает информацию о лоте в обоих языках для экспорта в JSON."""
    # Проверка отмены
    if cancel_event and cancel_event.is_set():
        raise InterruptedError("Процесс отменен пользователем")
    
    # Получаем данные для русской локали
    lot_page_ru, from_cache_ru = get_cached_locale_data(acc, lot.id, "ru")
    if not from_cache_ru:
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    
    # Проверка отмены после получения русской локали
    if cancel_event and cancel_event.is_set():
        raise InterruptedError("Процесс отменен пользователем")
    
    short_ru = "1"
    desc_ru = "1"
    if lot_page_ru:
        short_ru = lot_page_ru.short_description or lot.description or "1"
        desc_ru = lot_page_ru.full_description or "1"
    
    # Получаем данные для английской локали
    lot_page_en, from_cache_en = get_cached_locale_data(acc, lot.id, "en")
    if not from_cache_en:
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    
    # Проверка отмены после получения английской локали
    if cancel_event and cancel_event.is_set():
        raise InterruptedError("Процесс отменен пользователем")
    
    short_en = "1"
    desc_en = "1"
    if lot_page_en:
        short_en = lot_page_en.short_description or "1"
        desc_en = lot_page_en.full_description or "1"
    
    price_ = lot.price or 1.0
    price_str = str(int(price_)) if price_ == int(price_) else str(price_)
    node_id = lot.subcategory.id if lot.subcategory else 0
    sc_name_ru = lot.subcategory.name if lot.subcategory else "???"
    
    return {
        "query": "",
        "form_created_at": str(int(time.time())),
        "node_id": str(node_id),
        "location": "",
        "deleted": "",
        "fields[summary][ru]": short_ru,
        "fields[summary][en]": short_en,
        "fields[images]": "",
        "price": price_str,
        "amount": "1", # Исправлено с "999999" на "1"
        "active": "on",
        "fields[desc][ru]": desc_ru,
        "fields[desc][en]": desc_en,
        "fields[payment_msg][ru]": "1", # Исправлено с пустого на "1"
        "fields[payment_msg][en]": "1", # Исправлено с пустого на "1"
        "fields[type]": sc_name_ru
    }

def process_lot(acc: Account, lot: LotShortcut, progress_queue: List[Dict], cancel_event=None) -> Optional[Dict]:
    """
    Обрабатывает отдельный лот и возвращает данные для JSON.
    Добавляет информацию о прогрессе в очередь.
    """
    lot_id = lot.id
    try:
        # Проверяем отмену
        if cancel_event and cancel_event.is_set():
            progress_queue.append({"status": "canceled", "lot_id": lot_id})
            return None
            
        result = build_json_for_lot(acc, lot, cancel_event)
        progress_queue.append({"status": "success", "lot_id": lot_id})
        return result
    except InterruptedError:
        progress_queue.append({"status": "canceled", "lot_id": lot_id})
        return None
    except Exception as e:
        logger.error(f"Ошибка при обработке лота {lot_id}: {e}")
        progress_queue.append({"status": "error", "lot_id": lot_id, "error": str(e)})
        return None

def process_lots_parallel(acc: Account, lots: List[LotShortcut], chat_id: int, bot, cancel_event=None) -> List[Dict]:
    """
    Параллельно обрабатывает список лотов и периодически отправляет обновления.
    """
    output_data = []
    progress_queue = []
    message_id = None
    last_update_time = 0
    cancel_pressed = False
    
    # Для ускорения работы с большими списками - разделим лоты на примерно одинаковые по цене группы 
    # и начнем их обработку параллельно
    lots_with_price = [(lot, lot.price or 0.0) for lot in lots]
    lots_with_price.sort(key=lambda x: x[1])  # Сортировка по цене для более равномерного распределения
    balanced_lots = []
    
    # Распределяем лоты равномерно по пакетам
    for i in range(0, len(lots_with_price), BATCH_SIZE):
        batch = lots_with_price[i:i+BATCH_SIZE]
        balanced_lots.extend([lot for lot, _ in batch])
    
    # Создаем пул потоков с увеличенным количеством рабочих потоков
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Отправляем сообщение о прогрессе с кнопкой отмены
        progress_msg = bot.send_message(
            chat_id, 
            f"⏳ Подготовка к обработке {len(lots)} лотов...",
            reply_markup=cancel_button
        )
        message_id = progress_msg.message_id
        
        # Отправляем задачи на выполнение
        future_to_lot = {}
        for lot in balanced_lots:
            future = executor.submit(process_lot, acc, lot, progress_queue, cancel_event)
            future_to_lot[future] = lot
        
        total_lots = len(lots)
        completed = 0
        success_count = 0
        error_count = 0
        
        # Обновляем сообщение о прогрессе
        bot.edit_message_text(
            f"⏳ Обработка лотов: 0/{total_lots} (0%)\n"
            f"✅ Успешно: 0 | ❌ Ошибок: 0 | ⏱ Прошло: 0с",
            chat_id,
            message_id,
            reply_markup=cancel_button
        )
        
        start_time = time.time()
        last_update_time = start_time
        
        # Обрабатываем результаты по мере их поступления
        for future in concurrent.futures.as_completed(future_to_lot):
            # Проверяем отмену процесса
            if cancel_event and cancel_event.is_set():
                if not cancel_pressed:
                    bot.edit_message_text(
                        f"🚫 Отмена процесса... Пожалуйста, подождите.",
                        chat_id,
                        message_id
                    )
                    cancel_pressed = True
                
                # Отменяем оставшиеся задачи
                for f in future_to_lot:
                    if not f.done():
                        f.cancel()
                
                break
            
            lot = future_to_lot[future]
            
            try:
                result = future.result()
                if result:
                    output_data.append(result)
                    success_count += 1
                else:
                    # Проверяем, была ли отмена через статус в очереди прогресса
                    if any(item.get("status") == "canceled" and item.get("lot_id") == lot.id 
                           for item in progress_queue):
                        if not cancel_pressed:
                            bot.edit_message_text(
                                f"🚫 Отмена процесса... Пожалуйста, подождите.",
                                chat_id,
                                message_id
                            )
                            cancel_pressed = True
                        break
                    
                    error_count += 1
                completed += 1
                
                # Обновляем сообщение о прогрессе с определенной периодичностью
                current_time = time.time()
                elapsed = int(current_time - start_time)
                if current_time - last_update_time >= PROGRESS_UPDATE_INTERVAL or completed == total_lots:
                    percent = int(completed / total_lots * 100)
                    
                    # Оценка времени до завершения
                    if completed > 0 and completed < total_lots:
                        time_per_lot = (current_time - start_time) / completed
                        estimated_total = time_per_lot * total_lots
                        remaining = estimated_total - (current_time - start_time)
                        remaining_str = f" | ⏱ Осталось: ~{int(remaining)}с"
                    else:
                        remaining_str = ""
                    
                    try:
                        bot.edit_message_text(
                            f"⏳ Обработка лотов: {completed}/{total_lots} ({percent}%)\n"
                            f"✅ Успешно: {success_count} | ❌ Ошибок: {error_count} | ⏱ Прошло: {elapsed}с{remaining_str}",
                            chat_id,
                            message_id,
                            reply_markup=cancel_button
                        )
                        last_update_time = current_time
                    except Exception as e:
                        logger.error(f"Ошибка при обновлении сообщения о прогрессе: {e}")
            
            except Exception as e:
                logger.error(f"Ошибка при получении результата для лота {lot.id}: {e}")
                completed += 1
                error_count += 1
    
    # Финальное сообщение о завершении
    elapsed = int(time.time() - start_time)
    try:
        if cancel_event and cancel_event.is_set():
            bot.edit_message_text(
                f"🚫 Процесс отменен!\n"
                f"✅ Успешно обработано: {success_count}/{total_lots} лотов\n"
                f"⏱ Затраченное время: {elapsed}с",
                chat_id,
                message_id
            )
        else:
            bot.edit_message_text(
                f"✅ Обработка завершена: {success_count}/{total_lots} лотов успешно обработано.\n"
                f"❌ Ошибок: {error_count}\n"
                f"⏱ Общее время: {elapsed}с\n"
                f"⚡ Средняя скорость: {round(total_lots/max(elapsed, 1), 1)} лотов/сек",
                chat_id,
                message_id
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении финального сообщения: {e}")
    
    return output_data

def export_to_json(bot, chat_id: int, data: list[dict], username: str):
    """Сохраняем данные в JSON и отправляем файл."""
    if not data:
        bot.send_message(chat_id, "❗ Нет лотов для экспорта (пустой список).")
        return
    
    filename = random_filename(username)
    path_ = os.path.join("storage", "cache", filename)
    os.makedirs(os.path.dirname(path_), exist_ok=True)
    
    try:
        with open(path_, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка при записи JSON в файл {path_}: {e}")
        bot.send_message(chat_id, f"⚠️ Ошибка при создании файла экспорта: {e}")
        return
    
    try:
        with open(path_, "rb") as f:
            bot.send_document(chat_id, f, caption=f"✅ Выгружено {len(data)} лот(ов).")
    except Exception as e:
        logger.error(f"Ошибка при отправке файла {path_}: {e}")
        bot.send_message(chat_id, f"⚠️ Ошибка при отправке файла экспорта: {e}")

def process_profile(chat_id: int, user_id: int, cardinal: Cardinal):
    """
    Обрабатывает профиль пользователя FunPay и формирует JSON с лотами.
    Эта функция запускается в отдельном потоке.
    """
    bot = cardinal.telegram.bot
    cancel_event = None
    
    # Создаем событие для отмены и сохраняем его
    with active_users_lock:
        if chat_id in user_data:
            cancel_event = Event()
            user_data[chat_id]["cancel_event"] = cancel_event
    
    try:
        # Установка локали перед получением профиля
        cardinal.account.locale = "ru"
        profile = cardinal.account.get_user(user_id)
        logger.info(f"[Авто-Копирование] Получен профиль пользователя {user_id} (чат {chat_id}).")
    except Exception as e:
        bot.send_message(chat_id, f"⚠️ Ошибка get_user({user_id}): {e}")
        logger.error(f"[Авто-Копирование] Ошибка get_user({user_id}) для чата {chat_id}: {e}")
        return
    
    # Получаем список лотов
    try:
        lots = list(profile.get_lots())
        logger.info(f"[Авто-Копирование] Найдено {len(lots)} лотов у пользователя {user_id} (чат {chat_id}).")
        
        if not lots:
            bot.send_message(chat_id, "🙁 У пользователя нет публичных лотов.")
            logger.info(f"[Авто-Копирование] У пользователя {user_id} нет публичных лотов (чат {chat_id}).")
            return
        
        # Параллельная обработка лотов
        out_list = process_lots_parallel(cardinal.account, lots, chat_id, bot, cancel_event)
        
        # Если процесс не был отменен пользователем
        if not (cancel_event and cancel_event.is_set()):
            # Экспорт результатов в JSON
            export_to_json(bot, chat_id, out_list, profile.username)
            logger.info(f"[Авто-Копирование] Пользователь {chat_id} выгрузил все лоты.")
        
        # Очистка пользовательских данных и кеша
        if len(locale_cache) > CACHE_CLEANUP_THRESHOLD:
            cleanup_cache()
        
    except Exception as e:
        logger.error(f"[Авто-Копирование] Ошибка при обработке лотов для чата {chat_id}: {e}")
        bot.send_message(chat_id, f"⚠️ Произошла ошибка: {e}")

def cmd_steal_lots(m: Message, cardinal: Cardinal):
    """Обрабатывает команду /steal_lots и начинает процесс."""
    bot = cardinal.telegram.bot
    chat_id = m.chat.id
    
    # Проверяем, нет ли уже активного процесса для этого пользователя
    with active_users_lock:
        if chat_id in active_tasks and not active_tasks[chat_id].done():
            bot.send_message(chat_id, "⚠️ У вас уже есть активный процесс копирования. Дождитесь его завершения или используйте /cancel.")
            return
    
    user_data[chat_id] = {"step": STATE_WAIT_LINK}
    bot.send_message(
        chat_id,
        "🔎 Пришлите ссылку на профиль FunPay, с которого копировать лоты.\n"
        "Например: https://funpay.com/users/11506286/\n\n"
        "/cancel — отмена."
    )
    logger.info(f"[Авто-Копирование] Пользователь {chat_id} начал процесс копирования.")

def cmd_cancel(m: Message, cardinal: Cardinal):
    """Обрабатывает команду /cancel и отменяет текущий процесс."""
    bot = cardinal.telegram.bot
    chat_id = m.chat.id
    
    # Проверяем и отменяем активную задачу
    with active_users_lock:
        # Устанавливаем событие отмены, если оно существует
        if chat_id in user_data and user_data[chat_id].get("cancel_event"):
            user_data[chat_id]["cancel_event"].set()
            bot.send_message(chat_id, "🚫 Операция отменяется... Пожалуйста, подождите.")
            logger.info(f"[Авто-Копирование] Пользователь {chat_id} отменил активный процесс.")
            return
        
        # Если нет события отмены, но есть активная задача
        if chat_id in active_tasks and not active_tasks[chat_id].done():
            try:
                active_tasks[chat_id].cancel()
            except:
                pass
            active_tasks.pop(chat_id, None)
            bot.send_message(chat_id, "🚫 Процесс копирования отменен.")
            logger.info(f"[Авто-Копирование] Пользователь {chat_id} отменил активный процесс.")
            
    # Очищаем данные пользователя
    if chat_id in user_data:
        user_data.pop(chat_id, None)
        bot.send_message(chat_id, "🚫 Действие отменено.")
        logger.info(f"[Авто-Копирование] Пользователь {chat_id} отменил процесс.")
    else:
        bot.send_message(chat_id, "🚫 Нет активного процесса для отмены.")

def handle_callback(call, cardinal: Cardinal):
    """Обрабатывает callback-запросы от инлайн-кнопок"""
    bot = cardinal.telegram.bot
    chat_id = call.message.chat.id
    
    if call.data == "cancel_copy":
        with active_users_lock:
            # Устанавливаем событие отмены, если оно существует
            if chat_id in user_data and user_data[chat_id].get("cancel_event"):
                user_data[chat_id]["cancel_event"].set()
                bot.answer_callback_query(call.id, "Отмена процесса инициирована")
                logger.info(f"[Авто-Копирование] Пользователь {chat_id} отменил процесс через кнопку.")
                return
        
        bot.answer_callback_query(call.id, "Нет активного процесса копирования")

def handle_text(m: Message, cardinal: Cardinal):
    """Обрабатывает текстовые сообщения от пользователя."""
    bot = cardinal.telegram.bot
    chat_id = m.chat.id
    
    if chat_id not in user_data:
        return
    
    step = user_data[chat_id]["step"]
    if step == STATE_WAIT_LINK:
        link_ = m.text.strip()
        user_id = extract_user_id(link_)
        if not user_id:
            bot.send_message(chat_id, "❗ Не удалось извлечь ID из ссылки. /cancel — отмена.")
            logger.warning(f"[Авто-Копирование] Пользователь {chat_id} прислал некорректную ссылку: {link_}")
            user_data.pop(chat_id, None)
            return
        
        # Устанавливаем состояние в "обработка" чтобы избежать повторных запусков
        user_data[chat_id]["step"] = STATE_PROCESSING
        
        # Запускаем обработку в отдельном потоке через ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(process_profile, chat_id, user_id, cardinal)
            
            # Сохраняем future для возможности отмены
            with active_users_lock:
                active_tasks[chat_id] = future
                
            # Добавляем колбэк для очистки данных после завершения
            def cleanup_callback(fut):
                try:
                    with active_users_lock:
                        active_tasks.pop(chat_id, None)
                    user_data.pop(chat_id, None)
                except Exception as e:
                    logger.error(f"[Авто-Копирование] Ошибка очистки данных: {e}")
            
            future.add_done_callback(cleanup_callback)

def cmd_stats(m: Message, cardinal: Cardinal):
    """Показывает статистику по кешу и текущим процессам."""
    bot = cardinal.telegram.bot
    chat_id = m.chat.id
    
    # Собираем статистику по кешу
    with locale_cache_lock, stats_lock, active_users_lock:
        cache_size = len(locale_cache)
        active_processes = len(user_data)
        active_tasks_count = sum(1 for fut in active_tasks.values() if not fut.done())
        
        # Статистика кеша
        total_hits = sum(cache_hit_stats.values())
        total_misses = sum(cache_miss_stats.values())
        total_requests = total_hits + total_misses
        hit_rate = (total_hits / total_requests * 100) if total_requests > 0 else 0
        
        # Статистика по локалям
        ru_hits = cache_hit_stats.get("ru", 0)
        ru_misses = cache_miss_stats.get("ru", 0)
        ru_rate = (ru_hits / (ru_hits + ru_misses) * 100) if (ru_hits + ru_misses) > 0 else 0
        
        en_hits = cache_hit_stats.get("en", 0)
        en_misses = cache_miss_stats.get("en", 0)
        en_rate = (en_hits / (en_hits + en_misses) * 100) if (en_hits + en_misses) > 0 else 0
    
    stats_message = (
        f"📊 **Статистика Авто-Копирования v{VERSION}**\n\n"
        f"🔄 **Активность:**\n"
        f"- Активные процессы: {active_processes}\n"
        f"- Выполняющиеся задачи: {active_tasks_count}\n\n"
        f"💾 **Кеш данных:**\n"
        f"- Размер кеша: {cache_size} записей\n"
        f"- Общая эффективность: {hit_rate:.1f}% ({total_hits}/{total_requests})\n"
        f"- RU локаль: {ru_rate:.1f}% ({ru_hits}/{ru_hits + ru_misses})\n"
        f"- EN локаль: {en_rate:.1f}% ({en_hits}/{en_hits + en_misses})\n\n"
        f"⚙️ **Настройки:**\n"
        f"- Параллельные потоки: {MAX_WORKERS}\n"
        f"- Время жизни кеша: {LOCALE_CACHE_TTL} сек\n"
        f"- Задержка запросов: {REQUEST_DELAY_MIN}-{REQUEST_DELAY_MAX} сек\n"
        f"- Размер пакета: {BATCH_SIZE} лотов\n"
        f"- Макс. размер кеша: {MAX_CACHE_SIZE} записей"
    )
    
    bot.send_message(chat_id, stats_message, parse_mode="Markdown")
    logger.info(f"[Авто-Копирование] Пользователь {chat_id} запросил статистику.")

def cmd_clear_cache(m: Message, cardinal: Cardinal):
    """Очищает кеш локализаций."""
    bot = cardinal.telegram.bot
    chat_id = m.chat.id
    
    with locale_cache_lock:
        cache_size_before = len(locale_cache)
        locale_cache.clear()
    
    with stats_lock:
        cache_hit_stats.clear()
        cache_miss_stats.clear()
    
    bot.send_message(chat_id, f"✅ Кеш локализаций очищен. Удалено {cache_size_before} записей.")
    logger.info(f"[Авто-Копирование] Пользователь {chat_id} очистил кеш локализаций.")

def pingtest_cmd(m: Message, cardinal: Cardinal):
    """Тестовый хендлер для проверки работы бота."""
    bot = cardinal.telegram.bot
    chat_id = m.chat.id
    start_time = time.time()
    msg = bot.send_message(chat_id, "🏓 Измеряю задержку...")
    end_time = time.time()
    latency = int((end_time - start_time) * 1000)  # в миллисекундах
    
    bot.edit_message_text(f"🏓 Pong! Задержка: {latency} мс", chat_id, msg.message_id)
    logger.info(f"[Авто-Копирование] Пользователь {chat_id} выполнил /pingtest. Задержка: {latency} мс")

def init_plugin(cardinal: Cardinal, *args):
    bot = cardinal.telegram.bot
    
    cardinal.add_telegram_commands(UUID, [
        ("steal_lots", "🤖 Авто-Копирование лотов (RU+EN)", True),
        ("cancel", "🚫 Отмена", True),
        ("stats_copy", "📊 Статистика копирования", True),
        ("clear_cache", "🗑️ Очистить кеш локализаций", True),
        ("pingtest", "🏓 Проверка работы бота", True),
    ])
    
    @bot.message_handler(commands=["steal_lots"])
    def steal_cmd(m: Message):
        cmd_steal_lots(m, cardinal)
    
    @bot.message_handler(commands=["cancel"])
    def cancel_cmd(m: Message):
        cmd_cancel(m, cardinal)
    
    @bot.message_handler(commands=["stats_copy"])
    def stats_cmd(m: Message):
        cmd_stats(m, cardinal)
    
    @bot.message_handler(commands=["clear_cache"])
    def clear_cache_cmd(m: Message):
        cmd_clear_cache(m, cardinal)
    
    @bot.message_handler(commands=["pingtest"])
    def ping_cmd(m: Message):
        pingtest_cmd(m, cardinal)
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_copy"))
    def callback_handler(call):
        handle_callback(call, cardinal)
    
    @bot.message_handler(content_types=["text"])
    def text_msgs(m: Message):
        handle_text(m, cardinal)
    
    logger.info(f"[Авто-Копирование] Плагин инициализирован, версия {VERSION}")

BIND_TO_PRE_INIT = [init_plugin]
BIND_TO_DELETE = None