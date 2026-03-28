import asyncio
import csv
import hashlib
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, KeyboardButton, Message, ReplyKeyboardMarkup
from openpyxl import Workbook

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
TIMEZONE = ZoneInfo("Europe/Moscow")

DB_PATH = "bot.db"
LOG_PATH = "bot.log"

REMINDER_MINUTES = 10
DOUBLE_TAP_SECONDS = 1.2

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found in environment")
if not OWNER_ID:
    raise RuntimeError("OWNER_ID not found in environment")

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("value-bot")

# =========================
# BOT
# =========================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

# антидубль на быстрые нажатия
ACTION_GUARD = {}


# =========================
# STATES
# =========================
class ShiftState(StatesGroup):
    waiting_budget = State()
    waiting_bet_amount = State()
    waiting_end_shift_confirm = State()
    waiting_delete_last_confirm = State()
    waiting_delete_bet_number = State()
    waiting_result_bet_number = State()
    waiting_result_status = State()
    waiting_edit_stake_bet_number = State()
    waiting_edit_stake_value = State()


# =========================
# HELPERS
# =========================
def now_dt() -> datetime:
    return datetime.now(TIMEZONE)


def now_str() -> str:
    return now_dt().strftime("%Y-%m-%d %H:%M:%S")


def as_float(text: str) -> float:
    return float(text.replace(" ", "").replace(",", "."))


def connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def has_recent_action(user_id: int, action: str, seconds: float = DOUBLE_TAP_SECONDS) -> bool:
    key = f"{user_id}:{action}"
    now_ts = time.time()
    last_ts = ACTION_GUARD.get(key)
    ACTION_GUARD[key] = now_ts
    return last_ts is not None and (now_ts - last_ts) < seconds


def is_forward_message(message: Message) -> bool:
    return bool(
        getattr(message, "forward_origin", None)
        or getattr(message, "forward_from_chat", None)
        or getattr(message, "forward_from", None)
        or getattr(message, "forward_sender_name", None)
    )


def hash_text(text: str) -> str:
    return hashlib.md5(text.strip().encode("utf-8")).hexdigest()


def split_sport_tournament(header: str):
    parts = [x.strip() for x in re.split(r"\s+-\s+", header.strip()) if x.strip()]
    sport = parts[0] if parts else ""
    tournament = " - ".join(parts[1:]) if len(parts) > 1 else ""
    return sport, tournament


def parse_match_start(match_date: str, match_time: str) -> datetime:
    day, month = match_date.split("/")
    hour, minute = match_time.split(":")
    now = now_dt()

    dt = datetime(
        year=now.year,
        month=int(month),
        day=int(day),
        hour=int(hour),
        minute=int(minute),
        tzinfo=TIMEZONE,
    )

    if dt < now - timedelta(days=30):
        dt = dt.replace(year=now.year + 1)

    return dt


def calc_settlement(stake: float, odds: float, result_status: str):
    if result_status == "pending":
        return None, None

    if result_status == "win":
        payout = round(stake * odds, 2)
        profit = round(payout - stake, 2)
        return payout, profit

    if result_status == "lose":
        return 0.0, round(-stake, 2)

    if result_status == "half_win":
        payout = round((stake / 2) * odds + (stake / 2), 2)
        profit = round(payout - stake, 2)
        return payout, profit

    if result_status == "half_lose":
        payout = round(stake / 2, 2)
        profit = round(payout - stake, 2)
        return payout, profit

    if result_status == "refund":
        return round(stake, 2), 0.0

    return None, None


def calc_roi(total_profit: float, total_stake: float) -> float:
    if not total_stake:
        return 0.0
    return round((total_profit / total_stake) * 100, 2)


def save_log(level: str, message: str):
    db = connect()
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    db.execute(
        "INSERT INTO logs(level, message, created_at) VALUES (?, ?, ?)",
        (level, message, now_str()),
    )
    db.commit()
    db.close()


def log_info(text: str):
    logger.info(text)
    save_log("INFO", text)


def log_warning(text: str):
    logger.warning(text)
    save_log("WARNING", text)


def log_error(text: str):
    logger.error(text)
    save_log("ERROR", text)


# =========================
# DB
# =========================
def add_column_if_not_exists(table_name: str, column_name: str, ddl: str):
    db = connect()
    cols = db.execute(f"PRAGMA table_info({table_name})").fetchall()
    col_names = [c[1] for c in cols]
    if column_name not in col_names:
        db.execute(ddl)
        db.commit()
    db.close()


def init_db():
    db = connect()

    db.execute(
        """
    CREATE TABLE IF NOT EXISTS shifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        budget REAL NOT NULL,
        spent REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL
    )
    """
    )

    db.execute(
        """
    CREATE TABLE IF NOT EXISTS bets(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shift_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        sport TEXT,
        tournament TEXT,
        match_name TEXT,
        match_date TEXT,
        match_time TEXT,
        match_start_at TEXT,
        market TEXT,
        odds REAL,
        ev REAL,
        bookmaker TEXT,
        stake REAL,
        source_text TEXT,
        match_hash TEXT UNIQUE,
        reminder_sent INTEGER DEFAULT 0,
        result_status TEXT DEFAULT 'pending',
        payout REAL,
        profit REAL
    )
    """
    )

    db.execute(
        """
    CREATE TABLE IF NOT EXISTS logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        level TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """
    )

    db.commit()
    db.close()

    add_column_if_not_exists("bets", "match_start_at", "ALTER TABLE bets ADD COLUMN match_start_at TEXT")
    add_column_if_not_exists("bets", "reminder_sent", "ALTER TABLE bets ADD COLUMN reminder_sent INTEGER DEFAULT 0")
    add_column_if_not_exists("bets", "result_status", "ALTER TABLE bets ADD COLUMN result_status TEXT DEFAULT 'pending'")
    add_column_if_not_exists("bets", "payout", "ALTER TABLE bets ADD COLUMN payout REAL")
    add_column_if_not_exists("bets", "profit", "ALTER TABLE bets ADD COLUMN profit REAL")

    log_info("Database initialized")


def get_active_shift(user_id: int):
    db = connect()
    row = db.execute(
        """
        SELECT id, budget, spent, started_at
        FROM shifts
        WHERE user_id = ? AND status = 'active'
        ORDER BY id DESC
        LIMIT 1
    """,
        (user_id,),
    ).fetchone()
    db.close()
    return row


def start_shift_db(user_id: int, started_at: str, budget: float):
    db = connect()
    db.execute(
        """
        INSERT INTO shifts(user_id, started_at, budget, spent, status)
        VALUES (?, ?, ?, 0, 'active')
    """,
        (user_id, started_at, budget),
    )
    db.commit()
    db.close()
    log_info(f"Shift started | user={user_id} | budget={budget}")


def end_shift_db(shift_id: int, ended_at: str):
    db = connect()
    db.execute(
        """
        UPDATE shifts
        SET ended_at = ?, status = 'ended'
        WHERE id = ?
    """,
        (ended_at, shift_id),
    )
    db.commit()
    db.close()
    log_info(f"Shift ended | shift_id={shift_id}")


def add_bet_db(shift_id: int, user_id: int, created_at: str, parsed: dict, stake: float):
    db = connect()
    db.execute(
        """
        INSERT INTO bets(
            shift_id, user_id, created_at,
            sport, tournament, match_name, match_date, match_time, match_start_at,
            market, odds, ev, bookmaker, stake, source_text, match_hash,
            reminder_sent, result_status, payout, profit
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'pending', NULL, NULL)
    """,
        (
            shift_id,
            user_id,
            created_at,
            parsed["sport"],
            parsed["tournament"],
            parsed["match_name"],
            parsed["match_date"],
            parsed["match_time"],
            parsed["match_start_at"],
            parsed["market"],
            parsed["odds"],
            parsed["ev"],
            parsed["bookmaker"],
            stake,
            parsed["source_text"],
            parsed["hash"],
        ),
    )

    db.execute(
        """
        UPDATE shifts
        SET spent = spent + ?
        WHERE id = ?
    """,
        (stake, shift_id),
    )

    db.commit()
    db.close()
    log_info(f"Bet added | shift_id={shift_id} | user={user_id} | stake={stake}")


def get_last_bets(user_id: int, limit: int = 20):
    db = connect()
    rows = db.execute(
        """
        SELECT id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status
        FROM bets
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """,
        (user_id, limit),
    ).fetchall()
    db.close()
    return rows


def get_pending_bets(user_id: int, limit: int = 20):
    db = connect()
    rows = db.execute(
        """
        SELECT id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status
        FROM bets
        WHERE user_id = ? AND result_status = 'pending'
        ORDER BY id DESC
        LIMIT ?
    """,
        (user_id, limit),
    ).fetchall()
    db.close()
    return rows


def get_last_bet(user_id: int):
    rows = get_last_bets(user_id, 1)
    return rows[0] if rows else None


def get_bet_by_id(bet_id: int):
    db = connect()
    row = db.execute(
        """
        SELECT id, sport, tournament, match_name, match_date, match_time, market, odds, ev,
               bookmaker, stake, created_at, result_status, payout, profit, shift_id
        FROM bets
        WHERE id = ?
    """,
        (bet_id,),
    ).fetchone()
    db.close()
    return row


def count_bets_in_shift(shift_id: int) -> int:
    db = connect()
    row = db.execute("SELECT COUNT(*) FROM bets WHERE shift_id = ?", (shift_id,)).fetchone()
    db.close()
    return row[0] if row else 0


def get_shift_stats(shift_id: int):
    db = connect()
    row = db.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(stake), 0),
            COALESCE(AVG(odds), 0),
            COALESCE(AVG(ev), 0),
            COALESCE(SUM(CASE WHEN result_status = 'win' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'lose' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'half_win' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'half_lose' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'refund' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(profit), 0)
        FROM bets
        WHERE shift_id = ?
    """,
        (shift_id,),
    ).fetchone()
    db.close()
    return row


def get_today_stats(user_id: int):
    db = connect()
    start_day = now_dt().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    end_day = now_dt().replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    row = db.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(stake), 0),
            COALESCE(AVG(odds), 0),
            COALESCE(AVG(ev), 0),
            COALESCE(SUM(CASE WHEN result_status = 'win' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'lose' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'half_win' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'half_lose' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'refund' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(CASE WHEN result_status = 'pending' THEN 1 ELSE 0 END), 0),
            COALESCE(SUM(profit), 0)
        FROM bets
        WHERE user_id = ?
          AND created_at >= ?
          AND created_at <= ?
    """,
        (user_id, start_day, end_day),
    ).fetchone()
    db.close()
    return row


def update_bet_result(bet_id: int, result_status: str):
    db = connect()
    row = db.execute(
        """
        SELECT stake, odds
        FROM bets
        WHERE id = ?
    """,
        (bet_id,),
    ).fetchone()

    if not row:
        db.close()
        return False

    stake, odds = row
    payout, profit = calc_settlement(stake, odds, result_status)

    db.execute(
        """
        UPDATE bets
        SET result_status = ?, payout = ?, profit = ?
        WHERE id = ?
    """,
        (result_status, payout, profit, bet_id),
    )
    db.commit()
    db.close()

    log_info(f"Bet result updated | bet_id={bet_id} | status={result_status}")
    return True


def delete_bet_by_id(bet_id: int):
    db = connect()
    row = db.execute(
        """
        SELECT shift_id, stake
        FROM bets
        WHERE id = ?
    """,
        (bet_id,),
    ).fetchone()

    if not row:
        db.close()
        return False, None

    shift_id, stake = row

    db.execute("DELETE FROM bets WHERE id = ?", (bet_id,))
    db.execute("UPDATE shifts SET spent = spent - ? WHERE id = ?", (stake, shift_id))
    db.commit()
    db.close()

    log_warning(f"Bet deleted | bet_id={bet_id} | shift_id={shift_id} | stake={stake}")
    return True, stake


def update_bet_stake(bet_id: int, new_stake: float):
    db = connect()
    row = db.execute(
        """
        SELECT shift_id, stake, odds, result_status
        FROM bets
        WHERE id = ?
    """,
        (bet_id,),
    ).fetchone()

    if not row:
        db.close()
        return False, "Ставка не найдена."

    shift_id, old_stake, odds, result_status = row
    delta = new_stake - old_stake
    payout, profit = calc_settlement(new_stake, odds, result_status)

    db.execute(
        """
        UPDATE bets
        SET stake = ?, payout = ?, profit = ?
        WHERE id = ?
    """,
        (new_stake, payout, profit, bet_id),
    )
    db.execute("UPDATE shifts SET spent = spent + ? WHERE id = ?", (delta, shift_id))
    db.commit()
    db.close()

    log_info(f"Bet stake updated | bet_id={bet_id} | old={old_stake} | new={new_stake}")
    return True, old_stake


def export_bets_to_csv(user_id: int) -> str | None:
    db = connect()
    rows = db.execute(
        """
        SELECT
            id, shift_id, created_at, sport, tournament, match_name, match_date, match_time,
            match_start_at, market, odds, ev, bookmaker, stake, result_status, payout, profit
        FROM bets
        WHERE user_id = ?
        ORDER BY id DESC
    """,
        (user_id,),
    ).fetchall()
    db.close()

    if not rows:
        return None

    filename = f"bets_export_{now_dt().strftime('%Y%m%d_%H%M%S')}.csv"
    export_path = Path(filename)
    with export_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "id", "shift_id", "created_at", "sport", "tournament", "match_name",
            "match_date", "match_time", "match_start_at", "market", "odds", "ev",
            "bookmaker", "stake", "result_status", "payout", "profit"
        ])
        writer.writerows(rows)

    log_info(f"CSV exported | rows={len(rows)}")
    return str(export_path)


def export_bets_to_xlsx(user_id: int) -> str | None:
    db = connect()
    rows = db.execute(
        """
        SELECT
            id, shift_id, created_at, sport, tournament, match_name, match_date, match_time,
            match_start_at, market, odds, ev, bookmaker, stake, result_status, payout, profit
        FROM bets
        WHERE user_id = ?
        ORDER BY id DESC
    """,
        (user_id,),
    ).fetchall()
    db.close()

    if not rows:
        return None

    filename = f"bets_export_{now_dt().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Bets"

    headers = [
        "id", "shift_id", "created_at", "sport", "tournament", "match_name",
        "match_date", "match_time", "match_start_at", "market", "odds", "ev",
        "bookmaker", "stake", "result_status", "payout", "profit"
    ]
    ws.append(headers)

    for row in rows:
        ws.append(list(row))

    wb.save(filename)
    log_info(f"XLSX exported | rows={len(rows)}")
    return filename


def get_recent_logs(limit: int = 10):
    db = connect()
    rows = db.execute(
        """
        SELECT level, message, created_at
        FROM logs
        ORDER BY id DESC
        LIMIT ?
    """,
        (limit,),
    ).fetchall()
    db.close()
    return rows


def get_due_reminders():
    db = connect()
    rows = db.execute(
        """
        SELECT id, user_id, match_name, market, match_start_at, stake, odds, bookmaker
        FROM bets
        WHERE reminder_sent = 0
          AND result_status = 'pending'
          AND match_start_at IS NOT NULL
    """
    ).fetchall()
    db.close()

    now = now_dt()
    upper = now + timedelta(minutes=REMINDER_MINUTES)
    due = []

    for row in rows:
        bet_id, user_id, match_name, market, match_start_at, stake, odds, bookmaker = row
        try:
            dt = datetime.fromisoformat(match_start_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TIMEZONE)
        except Exception:
            continue

        if now <= dt <= upper:
            due.append(
                {
                    "id": bet_id,
                    "user_id": user_id,
                    "match_name": match_name,
                    "market": market,
                    "match_start_at": dt,
                    "stake": stake,
                    "odds": odds,
                    "bookmaker": bookmaker,
                }
            )

    return due


def mark_reminder_sent(bet_id: int):
    db = connect()
    db.execute("UPDATE bets SET reminder_sent = 1 WHERE id = ?", (bet_id,))
    db.commit()
    db.close()


# =========================
# PARSER
# =========================
def parse_bet(text: str):
    """
    Берём только первую корректную ставку из сообщения,
    даже если перед ней есть строки вроде:
    '‼️ Внимание! Ставка только для PRO'
    """
    pattern = re.compile(r"(⚽️🏒🎾.*?)(?=\n\s*⚽️🏒🎾|\Z)", re.S)
    matches = pattern.findall(text)

    if not matches:
        return None

    block = None

    for candidate in matches:
        sport_line = re.search(r"⚽️🏒🎾\s*(.+?)\n", candidate, re.S)
        event_line = re.search(r"🚩\s*(.+?),\s*(\d{1,2}:\d{2})\s+(\d{2}/\d{2})", candidate)
        market_line = re.search(r"❗️\s*(.+?)\s*коэф\.?\s*([\d.,]+)❗️", candidate, re.S)

        if sport_line and event_line and market_line:
            block = candidate.strip()
            break

    if not block:
        return None

    sport_line = re.search(r"⚽️🏒🎾\s*(.+?)\n", block, re.S)
    event_line = re.search(r"🚩\s*(.+?),\s*(\d{1,2}:\d{2})\s+(\d{2}/\d{2})", block)
    market_line = re.search(r"❗️\s*(.+?)\s*коэф\.?\s*([\d.,]+)❗️", block, re.S)
    ev_line = re.search(r"Математическое ожидание\s*≈\s*([\d.,]+)%", block, re.I)
    bk_line = re.search(r"Ставка сделана👉\s*([^\n(]+)", block, re.I)

    if not sport_line or not event_line or not market_line:
        return None

    full_header = sport_line.group(1).strip()
    sport, tournament = split_sport_tournament(full_header)

    match_name = event_line.group(1).strip()
    match_time = event_line.group(2).strip()
    match_date = event_line.group(3).strip()
    market = re.sub(r"\s+", " ", market_line.group(1)).strip()
    odds = float(market_line.group(2).replace(",", "."))
    ev = float(ev_line.group(1).replace(",", ".")) if ev_line else None
    bookmaker = bk_line.group(1).strip() if bk_line else ""
    match_start_at = parse_match_start(match_date, match_time)

    return {
        "sport": sport,
        "tournament": tournament,
        "match_name": match_name,
        "match_time": match_time,
        "match_date": match_date,
        "match_start_at": match_start_at.isoformat(),
        "market": market,
        "odds": odds,
        "ev": ev,
        "bookmaker": bookmaker,
        "hash": hash_text(block),
        "source_text": block,
    }


# =========================
# LABELS
# =========================
RESULT_MAP = {
    "🕒 В ожидании": "pending",
    "✅ Выигрыш": "win",
    "❌ Проигрыш": "lose",
    "🟡 Половина выигрыша": "half_win",
    "🟠 Половина проигрыша": "half_lose",
    "↩️ Возврат": "refund",
}

RESULT_LABELS = {
    "pending": "🕒 В ожидании",
    "win": "✅ Выигрыш",
    "lose": "❌ Проигрыш",
    "half_win": "🟡 Половина выигрыша",
    "half_lose": "🟠 Половина проигрыша",
    "refund": "↩️ Возврат",
}


# =========================
# KEYBOARDS
# =========================
def main_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎯 Смена"), KeyboardButton(text="📚 Ставки")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Сервис")],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def shift_menu_kb(active_shift_exists: bool):
    keyboard = []
    if not active_shift_exists:
        keyboard.append([KeyboardButton(text="🚀 Начать смену")])
    else:
        keyboard.append([KeyboardButton(text="📍 Текущая смена"), KeyboardButton(text="🏁 Завершить смену")])

    keyboard.append([KeyboardButton(text="⬅️ Назад")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        is_persistent=True,
    )


def bets_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить ставку"), KeyboardButton(text="🧾 Последняя ставка")],
            [KeyboardButton(text="📚 Последние 20 ставок")],
            [KeyboardButton(text="🏷 Отметить результат"), KeyboardButton(text="🗑 Delete last")],
            [KeyboardButton(text="✏️ Исправить сумму")],
            [KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def stats_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📈 Статистика по смене"), KeyboardButton(text="📅 Статистика за день")],
            [KeyboardButton(text="📤 Export CSV"), KeyboardButton(text="📦 Export XLSX")],
            [KeyboardButton(text="📌 Ближайшие матчи")],
            [KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def service_menu_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Логи")],
            [KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def amount_retry_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔁 Повторить ввод суммы")],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def yes_no_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Подтвердить"), KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def result_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🕒 В ожидании")],
            [KeyboardButton(text="✅ Выигрыш"), KeyboardButton(text="❌ Проигрыш")],
            [KeyboardButton(text="🟡 Половина выигрыша"), KeyboardButton(text="🟠 Половина проигрыша")],
            [KeyboardButton(text="↩️ Возврат")],
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


# =========================
# COMMANDS
# =========================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("⛔ Этот бот доступен только владельцу.")
        return

    await state.clear()
    await message.answer(
        "🚀 <b>Бот учёта ставок запущен</b>\n\n"
        "Я помогу тебе:\n"
        "• вести смену\n"
        "• контролировать бюджет\n"
        "• сохранять ставки\n"
        "• напоминать о матчах\n"
        "• считать статистику\n\n"
        "Выбери раздел 👇",
        reply_markup=main_menu_kb(),
    )


# =========================
# ROOT NAVIGATION
# =========================
@dp.message(F.text == "⬅️ Назад")
async def back_to_main(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Главное меню", reply_markup=main_menu_kb())


@dp.message(F.text == "❌ Отмена")
async def cancel_action(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Действие отменено.", reply_markup=main_menu_kb())


@dp.message(F.text == "🎯 Смена")
async def open_shift_menu(message: Message, state: FSMContext):
    await state.clear()
    active = get_active_shift(message.from_user.id)
    if active:
        _, budget, spent, started_at = active
        remain = round(budget - spent, 2)
        await message.answer(
            f"🟢 <b>Смена активна</b>\n\n"
            f"🕒 Начало: <b>{started_at} МСК</b>\n"
            f"💰 Бюджет: <b>{budget}</b>\n"
            f"💸 Поставлено: <b>{spent}</b>\n"
            f"🟢 Остаток: <b>{remain}</b>",
            reply_markup=shift_menu_kb(True),
        )
    else:
        await message.answer(
            "🎯 <b>Раздел «Смена»</b>\n\nЗдесь ты можешь начать или завершить смену.",
            reply_markup=shift_menu_kb(False),
        )


@dp.message(F.text == "📚 Ставки")
async def open_bets_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "📚 <b>Раздел «Ставки»</b>\n\nЗдесь можно добавлять ставки, смотреть историю и отмечать результаты.",
        reply_markup=bets_menu_kb(),
    )


@dp.message(F.text == "📊 Статистика")
async def open_stats_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "📊 <b>Раздел «Статистика»</b>\n\nЗдесь можно смотреть статистику по смене и выгружать отчёты.",
        reply_markup=stats_menu_kb(),
    )


@dp.message(F.text == "⚙️ Сервис")
async def open_service_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "⚙️ <b>Раздел «Сервис»</b>\n\nЗдесь собраны служебные функции.",
        reply_markup=service_menu_kb(),
    )


# =========================
# SHIFT FLOW
# =========================
@dp.message(F.text == "🚀 Начать смену")
async def start_shift_button(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "start_shift"):
        return

    active = get_active_shift(message.from_user.id)
    if active:
        shift_id, budget, spent, started_at = active
        remain = round(budget - spent, 2)
        bets_count = count_bets_in_shift(shift_id)
        await message.answer(
            f"ℹ️ <b>Смена уже активна</b>\n\n"
            f"🕒 Начало: <b>{started_at} МСК</b>\n"
            f"💰 Бюджет: <b>{budget}</b>\n"
            f"💸 Поставлено: <b>{spent}</b>\n"
            f"🎯 Ставок в смене: <b>{bets_count}</b>\n"
            f"🟢 Остаток: <b>{remain}</b>",
            reply_markup=shift_menu_kb(True),
        )
        return

    await state.set_state(ShiftState.waiting_budget)
    await message.answer(
        "💰 <b>Введи бюджет смены</b>\n\nПример: <code>10000</code>",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            is_persistent=True,
        ),
    )


@dp.message(ShiftState.waiting_budget)
async def budget_input(message: Message, state: FSMContext):
    try:
        budget = as_float(message.text.strip())
        if budget <= 0:
            raise ValueError
    except Exception:
        await message.answer(
            "⚠️ Бюджет не распознан.\nВведи число, например: <code>10000</code>",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True,
                is_persistent=True,
            ),
        )
        return

    start_shift_db(message.from_user.id, now_str(), budget)
    await state.clear()
    await message.answer(
        f"✅ <b>Смена начата</b>\n\n"
        f"🟢 Статус: <b>Смена активна</b>\n"
        f"💰 Бюджет: <b>{budget}</b>\n"
        f"🕒 Время старта: <b>{now_str()} МСК</b>",
        reply_markup=shift_menu_kb(True),
    )


@dp.message(F.text == "📍 Текущая смена")
async def current_shift_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "current_shift"):
        return

    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("📭 Активной смены нет.", reply_markup=shift_menu_kb(False))
        return

    shift_id, budget, spent, started_at = active
    remain = round(budget - spent, 2)
    bets_count = count_bets_in_shift(shift_id)

    await message.answer(
        f"📊 <b>Текущая смена</b>\n\n"
        f"🕒 Начало: <b>{started_at} МСК</b>\n"
        f"💰 Бюджет: <b>{budget}</b>\n"
        f"💸 Поставлено: <b>{spent}</b>\n"
        f"🎯 Ставок в смене: <b>{bets_count}</b>\n"
        f"🟢 Остаток: <b>{remain}</b>",
        reply_markup=shift_menu_kb(True),
    )


@dp.message(F.text == "🏁 Завершить смену")
async def end_shift_handler(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "end_shift"):
        return

    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("📭 Активной смены нет.", reply_markup=shift_menu_kb(False))
        return

    shift_id, budget, spent, started_at = active
    remain = round(budget - spent, 2)
    bets_count = count_bets_in_shift(shift_id)

    await state.set_state(ShiftState.waiting_end_shift_confirm)
    await message.answer(
        f"🏁 <b>Подтвердить завершение смены?</b>\n\n"
        f"💰 Бюджет: <b>{budget}</b>\n"
        f"💸 Поставлено: <b>{spent}</b>\n"
        f"🎯 Ставок: <b>{bets_count}</b>\n"
        f"🟢 Остаток: <b>{remain}</b>",
        reply_markup=yes_no_kb(),
    )


@dp.message(ShiftState.waiting_end_shift_confirm, F.text == "✅ Подтвердить")
async def confirm_end_shift(message: Message, state: FSMContext):
    active = get_active_shift(message.from_user.id)
    if not active:
        await state.clear()
        await message.answer("📭 Активной смены уже нет.", reply_markup=shift_menu_kb(False))
        return

    shift_id, budget, spent, started_at = active
    remain = round(budget - spent, 2)
    bets_count = count_bets_in_shift(shift_id)
    end_shift_db(shift_id, now_str())
    await state.clear()

    await message.answer(
        f"🏁 <b>Смена завершена</b>\n\n"
        f"🕒 Начало: <b>{started_at} МСК</b>\n"
        f"🕒 Конец: <b>{now_str()} МСК</b>\n"
        f"💰 Бюджет: <b>{budget}</b>\n"
        f"💸 Поставлено: <b>{spent}</b>\n"
        f"🎯 Ставок: <b>{bets_count}</b>\n"
        f"🟢 Остаток: <b>{remain}</b>",
        reply_markup=shift_menu_kb(False),
    )


# =========================
# BETS MENU
# =========================
@dp.message(F.text == "➕ Добавить ставку")
async def add_bet_hint(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("⚠️ Сначала начни смену.", reply_markup=shift_menu_kb(False))
        return

    await message.answer(
        "📥 <b>Перешли мне сообщение со ставкой</b>\n\n"
        "Важно:\n"
        "• только пересланное сообщение\n"
        "• бот берёт первую корректную ставку из сообщения\n"
        "• если формат не распознается, я так и напишу",
        reply_markup=bets_menu_kb(),
    )


@dp.message(F.text == "🧾 Последняя ставка")
async def last_bet_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "last_bet"):
        return

    row = get_last_bet(message.from_user.id)
    if not row:
        await message.answer("📭 Пока нет ставок.", reply_markup=bets_menu_kb())
        return

    bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row

    await message.answer(
        f"🧾 <b>Последняя ставка</b>\n\n"
        f"🏅 {sport}\n"
        f"🏟 <b>{match_name}</b>\n"
        f"📌 {market}\n"
        f"📈 КФ: <b>{odds}</b>\n"
        f"💸 Сумма: <b>{stake}</b>\n"
        f"🏦 БК: <b>{bookmaker}</b>\n"
        f"🏷 Статус: <b>{RESULT_LABELS.get(result_status, result_status)}</b>\n"
        f"🕒 Добавлена: <b>{created_at} МСК</b>",
        reply_markup=bets_menu_kb(),
    )


@dp.message(F.text == "📚 Последние 20 ставок")
async def last_20_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "last_20"):
        return

    rows = get_last_bets(message.from_user.id, 20)
    if not rows:
        await message.answer("📭 Пока нет ставок.", reply_markup=bets_menu_kb())
        return

    lines = []
    for i, row in enumerate(rows, start=1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        lines.append(
            f"{i}. <b>{sport}</b> | {match_name}\n"
            f"📌 {market}\n"
            f"📈 КФ: <b>{odds}</b> | 💸 {stake} | 🏦 {bookmaker}\n"
            f"🏷 <b>{RESULT_LABELS.get(result_status, result_status)}</b>"
        )

    await message.answer("\n\n".join(lines), reply_markup=bets_menu_kb())


@dp.message(F.text == "🗑 Delete last")
async def delete_last_handler(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    row = get_last_bet(message.from_user.id)
    if not row:
        await message.answer("📭 Нет ставок для удаления.", reply_markup=bets_menu_kb())
        return

    bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
    await state.update_data(delete_bet_id=bet_id)
    await state.set_state(ShiftState.waiting_delete_last_confirm)

    await message.answer(
        f"🗑 <b>Удалить последнюю ставку?</b>\n\n"
        f"🏟 Матч: <b>{match_name}</b>\n"
        f"📌 Маркет: {market}\n"
        f"💸 Сумма: <b>{stake}</b>\n"
        f"📈 КФ: <b>{odds}</b>\n"
        f"🏦 БК: <b>{bookmaker}</b>\n"
        f"🏷 Статус: <b>{RESULT_LABELS.get(result_status, result_status)}</b>",
        reply_markup=yes_no_kb(),
    )


@dp.message(ShiftState.waiting_delete_last_confirm, F.text == "✅ Подтвердить")
async def confirm_delete_last(message: Message, state: FSMContext):
    data = await state.get_data()
    bet_id = data.get("delete_bet_id")

    if not bet_id:
        await state.clear()
        await message.answer("⚠️ Не нашёл ставку для удаления.", reply_markup=bets_menu_kb())
        return

    ok, stake = delete_bet_by_id(bet_id)
    await state.clear()

    if not ok:
        await message.answer("⚠️ Не удалось удалить ставку.", reply_markup=bets_menu_kb())
        return

    await message.answer(
        f"🗑 <b>Ставка удалена</b>\n\n💸 Возвращено в расход смены: <b>{stake}</b>",
        reply_markup=bets_menu_kb(),
    )


@dp.message(F.text == "✏️ Исправить сумму")
async def edit_stake_start(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    rows = get_last_bets(message.from_user.id, 20)
    if not rows:
        await message.answer("📭 Нет ставок для редактирования.", reply_markup=bets_menu_kb())
        return

    lines = ["✏️ <b>Выбери ставку для изменения суммы</b>\n"]
    mapping = {}
    for idx, row in enumerate(rows, start=1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        mapping[str(idx)] = bet_id
        lines.append(
            f"{idx}. <b>{match_name}</b>\n"
            f"📌 {market}\n"
            f"💸 {stake} | 📈 {odds}\n"
            f"🏷 {RESULT_LABELS.get(result_status, result_status)}\n"
        )

    await state.update_data(edit_stake_choices=mapping)
    await state.set_state(ShiftState.waiting_edit_stake_bet_number)
    await message.answer(
        "\n".join(lines) + "\nНапиши номер ставки сообщением.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            is_persistent=True,
        ),
    )


@dp.message(ShiftState.waiting_edit_stake_bet_number)
async def edit_stake_choose_number(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    data = await state.get_data()
    mapping = data.get("edit_stake_choices", {})

    if text not in mapping:
        await message.answer("⚠️ Номер не найден. Напиши номер ставки из списка.")
        return

    bet_id = mapping[text]
    bet = get_bet_by_id(bet_id)
    if not bet:
        await state.clear()
        await message.answer("⚠️ Не удалось найти ставку.", reply_markup=bets_menu_kb())
        return

    (
        _id, sport, tournament, match_name, match_date, match_time, market,
        odds, ev, bookmaker, stake, created_at, result_status, payout, profit, shift_id
    ) = bet

    await state.update_data(edit_stake_bet_id=bet_id)
    await state.set_state(ShiftState.waiting_edit_stake_value)
    await message.answer(
        f"✏️ <b>Изменение суммы ставки</b>\n\n"
        f"🏟 <b>{match_name}</b>\n"
        f"📌 {market}\n"
        f"💸 Текущая сумма: <b>{stake}</b>\n\n"
        f"Напиши новую сумму.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            is_persistent=True,
        ),
    )


@dp.message(ShiftState.waiting_edit_stake_value)
async def edit_stake_value(message: Message, state: FSMContext):
    data = await state.get_data()
    bet_id = data.get("edit_stake_bet_id")
    if not bet_id:
        await state.clear()
        await message.answer("⚠️ Не выбрана ставка.", reply_markup=bets_menu_kb())
        return

    try:
        new_stake = as_float((message.text or "").strip())
        if new_stake <= 0:
            raise ValueError
    except Exception:
        await message.answer("⚠️ Сумма не распознана. Введи число, например: <code>1500</code>")
        return

    ok, old_stake = update_bet_stake(bet_id, new_stake)
    await state.clear()

    if not ok:
        await message.answer(f"⚠️ {old_stake}", reply_markup=bets_menu_kb())
        return

    await message.answer(
        f"✅ <b>Сумма обновлена</b>\n\nСтарая: <b>{old_stake}</b>\nНовая: <b>{new_stake}</b>",
        reply_markup=bets_menu_kb(),
    )


# =========================
# RESULT FLOW
# =========================
@dp.message(F.text == "🏷 Отметить результат")
async def mark_result_start(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    rows = get_pending_bets(message.from_user.id, 20)
    if not rows:
        await message.answer("📭 Нет ожидающих ставок для отметки результата.", reply_markup=bets_menu_kb())
        return

    lines = ["🏷 <b>Выбери ставку по номеру</b>\n"]
    mapping = {}

    for idx, row in enumerate(rows, start=1):
        bet_id, sport, match_name, market, odds, stake, bookmaker, created_at, result_status = row
        mapping[str(idx)] = bet_id
        lines.append(
            f"{idx}. <b>{match_name}</b>\n"
            f"📌 {market}\n"
            f"💸 {stake} | 📈 {odds}\n"
            f"🏷 {RESULT_LABELS.get(result_status, result_status)}\n"
        )

    await state.update_data(result_choices=mapping)
    await state.set_state(ShiftState.waiting_result_bet_number)

    await message.answer(
        "\n".join(lines) + "\nНапиши номер ставки сообщением.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True,
            is_persistent=True,
        ),
    )


@dp.message(ShiftState.waiting_result_bet_number)
async def result_bet_number_input(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    data = await state.get_data()
    mapping = data.get("result_choices", {})

    if text not in mapping:
        await message.answer("⚠️ Номер не найден. Напиши номер ставки из списка.")
        return

    bet_id = mapping[text]
    bet = get_bet_by_id(bet_id)
    if not bet:
        await state.clear()
        await message.answer("⚠️ Не удалось найти ставку.", reply_markup=bets_menu_kb())
        return

    (
        _id, sport, tournament, match_name, match_date, match_time, market,
        odds, ev, bookmaker, stake, created_at, result_status, payout, profit, shift_id
    ) = bet

    await state.update_data(selected_bet_id=bet_id)
    await state.set_state(ShiftState.waiting_result_status)

    await message.answer(
        f"🎯 <b>Выбрана ставка</b>\n\n"
        f"🏟 <b>{match_name}</b>\n"
        f"📌 {market}\n"
        f"💸 Сумма: <b>{stake}</b>\n"
        f"📈 КФ: <b>{odds}</b>\n"
        f"🏦 БК: <b>{bookmaker}</b>\n"
        f"🏷 Текущий статус: <b>{RESULT_LABELS.get(result_status, result_status)}</b>\n\n"
        f"Теперь выбери новый результат:",
        reply_markup=result_kb(),
    )


@dp.message(ShiftState.waiting_result_status, F.text.in_(list(RESULT_MAP.keys())))
async def set_result_handler(message: Message, state: FSMContext):
    result_status = RESULT_MAP[message.text]
    data = await state.get_data()
    bet_id = data.get("selected_bet_id")

    if not bet_id:
        await state.clear()
        await message.answer("⚠️ Не выбрана ставка.", reply_markup=bets_menu_kb())
        return

    ok = update_bet_result(bet_id, result_status)
    await state.clear()

    if not ok:
        await message.answer("⚠️ Не удалось обновить результат.", reply_markup=bets_menu_kb())
        return

    bet = get_bet_by_id(bet_id)
    if bet:
        (
            _id, sport, tournament, match_name, match_date, match_time, market,
            odds, ev, bookmaker, stake, created_at, result_status_db, payout, profit, shift_id
        ) = bet

        extra = ""
        if payout is not None:
            extra += f"\n💰 Выплата: <b>{payout}</b>"
        if profit is not None:
            extra += f"\n📊 Прибыль: <b>{profit}</b>"

        await message.answer(
            f"✅ <b>Результат обновлён</b>\n\n"
            f"🏟 <b>{match_name}</b>\n"
            f"🏷 Новый статус: <b>{RESULT_LABELS.get(result_status_db, result_status_db)}</b>{extra}",
            reply_markup=bets_menu_kb(),
        )
    else:
        await message.answer("✅ Результат обновлён.", reply_markup=bets_menu_kb())


# =========================
# STATS MENU
# =========================
@dp.message(F.text == "📈 Статистика по смене")
async def shift_stats_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "shift_stats"):
        return

    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("📭 Активной смены нет.", reply_markup=stats_menu_kb())
        return

    shift_id, budget, spent, started_at = active
    stats = get_shift_stats(shift_id)

    (
        total_bets, total_stake, avg_odds, avg_ev,
        wins, loses, half_wins, half_loses, refunds, pendings, total_profit
    ) = stats

    remain = round(budget - spent, 2)
    roi = calc_roi(total_profit, total_stake)

    await message.answer(
        f"📈 <b>Статистика по смене</b>\n\n"
        f"🎯 Ставок: <b>{total_bets}</b>\n"
        f"💸 Общая сумма: <b>{round(total_stake, 2)}</b>\n"
        f"📈 Средний КФ: <b>{round(avg_odds, 2) if total_bets else 0}</b>\n"
        f"🧠 Среднее мат. ожидание: <b>{round(avg_ev, 2) if total_bets else 0}</b>\n\n"
        f"✅ Выигрыш: <b>{wins}</b>\n"
        f"❌ Проигрыш: <b>{loses}</b>\n"
        f"🟡 Половина выигрыша: <b>{half_wins}</b>\n"
        f"🟠 Половина проигрыша: <b>{half_loses}</b>\n"
        f"↩️ Возврат: <b>{refunds}</b>\n"
        f"🕒 В ожидании: <b>{pendings}</b>\n\n"
        f"💰 Бюджет: <b>{budget}</b>\n"
        f"💸 Поставлено: <b>{spent}</b>\n"
        f"🟢 Остаток: <b>{remain}</b>\n"
        f"📊 Прибыль по отмеченным: <b>{round(total_profit, 2)}</b>\n"
        f"📐 ROI: <b>{roi}%</b>",
        reply_markup=stats_menu_kb(),
    )


@dp.message(F.text == "📅 Статистика за день")
async def today_stats_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    if has_recent_action(message.from_user.id, "today_stats"):
        return

    stats = get_today_stats(message.from_user.id)

    (
        total_bets, total_stake, avg_odds, avg_ev,
        wins, loses, half_wins, half_loses, refunds, pendings, total_profit
    ) = stats

    roi = calc_roi(total_profit, total_stake)

    await message.answer(
        f"📅 <b>Статистика за сегодня</b>\n\n"
        f"🎯 Ставок: <b>{total_bets}</b>\n"
        f"💸 Общая сумма: <b>{round(total_stake, 2)}</b>\n"
        f"📈 Средний КФ: <b>{round(avg_odds, 2) if total_bets else 0}</b>\n"
        f"🧠 Среднее мат. ожидание: <b>{round(avg_ev, 2) if total_bets else 0}</b>\n\n"
        f"✅ Выигрыш: <b>{wins}</b>\n"
        f"❌ Проигрыш: <b>{loses}</b>\n"
        f"🟡 Половина выигрыша: <b>{half_wins}</b>\n"
        f"🟠 Половина проигрыша: <b>{half_loses}</b>\n"
        f"↩️ Возврат: <b>{refunds}</b>\n"
        f"🕒 В ожидании: <b>{pendings}</b>\n\n"
        f"📊 Прибыль по отмеченным: <b>{round(total_profit, 2)}</b>\n"
        f"📐 ROI: <b>{roi}%</b>",
        reply_markup=stats_menu_kb(),
    )


@dp.message(F.text == "📤 Export CSV")
async def export_csv_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    path = export_bets_to_csv(message.from_user.id)
    if not path:
        await message.answer("📭 Пока нет данных для экспорта.", reply_markup=stats_menu_kb())
        return

    await message.answer_document(FSInputFile(path), caption="📤 Экспорт CSV готов.", reply_markup=stats_menu_kb())


@dp.message(F.text == "📦 Export XLSX")
async def export_xlsx_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    path = export_bets_to_xlsx(message.from_user.id)
    if not path:
        await message.answer("📭 Пока нет данных для экспорта.", reply_markup=stats_menu_kb())
        return

    await message.answer_document(FSInputFile(path), caption="📦 Экспорт XLSX готов.", reply_markup=stats_menu_kb())


@dp.message(F.text == "📌 Ближайшие матчи")
async def upcoming_matches_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    db = connect()
    rows = db.execute(
        """
        SELECT match_name, market, stake, odds, bookmaker, match_start_at
        FROM bets
        WHERE user_id = ? AND result_status = 'pending' AND match_start_at IS NOT NULL
        ORDER BY match_start_at ASC
        LIMIT 10
    """,
        (message.from_user.id,),
    ).fetchall()
    db.close()

    future_rows = []
    for row in rows:
        match_name, market, stake, odds, bookmaker, match_start_at = row
        try:
            dt = datetime.fromisoformat(match_start_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TIMEZONE)
        except Exception:
            continue
        if dt >= now_dt():
            future_rows.append((match_name, market, stake, odds, bookmaker, dt))

    if not future_rows:
        await message.answer("📭 Ближайших ожидающих матчей нет.", reply_markup=stats_menu_kb())
        return

    lines = ["📌 <b>Ближайшие матчи</b>\n"]
    for idx, row in enumerate(future_rows[:10], start=1):
        match_name, market, stake, odds, bookmaker, dt = row
        lines.append(
            f"{idx}. <b>{match_name}</b>\n"
            f"📌 {market}\n"
            f"💸 {stake} | 📈 {odds} | 🏦 {bookmaker}\n"
            f"🕒 {dt.astimezone(TIMEZONE).strftime('%d.%m.%Y %H:%M')} МСК\n"
        )

    await message.answer("\n".join(lines), reply_markup=stats_menu_kb())


# =========================
# SERVICE MENU
# =========================
@dp.message(F.text == "📋 Логи")
async def logs_handler(message: Message):
    if message.from_user.id != OWNER_ID:
        return

    rows = get_recent_logs(10)
    if not rows:
        await message.answer("📭 Логов пока нет.", reply_markup=service_menu_kb())
        return

    lines = ["📋 <b>Последние логи</b>\n"]
    for level, text, created_at in rows:
        lines.append(f"<b>{level}</b> | {created_at}\n{text}\n")

    await message.answer("\n".join(lines), reply_markup=service_menu_kb())


# =========================
# BET INPUT FLOW
# =========================
@dp.message(F.text == "🔁 Повторить ввод суммы")
async def retry_amount_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state != ShiftState.waiting_bet_amount.state:
        await message.answer("ℹ️ Сейчас нет активного ввода суммы.", reply_markup=bets_menu_kb())
        return

    await message.answer(
        "🔁 <b>Повтори ввод суммы</b>\n\nПример: <code>1500</code>",
        reply_markup=amount_retry_kb(),
    )


@dp.message(F.text)
async def universal_text_handler(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    text = (message.text or "").strip()
    current_state = await state.get_state()

    if current_state in {
        ShiftState.waiting_budget.state,
        ShiftState.waiting_end_shift_confirm.state,
        ShiftState.waiting_delete_last_confirm.state,
        ShiftState.waiting_delete_bet_number.state,
        ShiftState.waiting_result_bet_number.state,
        ShiftState.waiting_result_status.state,
        ShiftState.waiting_edit_stake_bet_number.state,
        ShiftState.waiting_edit_stake_value.state,
    }:
        return

    if current_state == ShiftState.waiting_bet_amount.state:
        data = await state.get_data()
        pending = data.get("pending_bet")

        if not pending:
            await state.clear()
            await message.answer("⚠️ Не нашёл ожидаемую ставку. Начни заново.", reply_markup=bets_menu_kb())
            return

        if text == "🔁 Повторить ввод суммы":
            return

        try:
            amount = as_float(text)
            if amount <= 0:
                raise ValueError
        except Exception:
            await message.answer(
                "⚠️ Сумма не распознана.\nВведи число, например: <code>1500</code>",
                reply_markup=amount_retry_kb(),
            )
            return

        active = get_active_shift(message.from_user.id)
        if not active:
            await state.clear()
            await message.answer("📭 Активной смены нет.", reply_markup=shift_menu_kb(False))
            return

        shift_id, budget, spent, started_at = active

        try:
            add_bet_db(shift_id, message.from_user.id, now_str(), pending, amount)
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                await state.clear()
                await message.answer("⚠️ Эта ставка уже была добавлена ранее.", reply_markup=bets_menu_kb())
                return

            log_error(f"Bet insert failed: {e}")
            await state.clear()
            await message.answer(f"❌ Ошибка записи ставки:\n<code>{e}</code>", reply_markup=bets_menu_kb())
            return

        new_spent = round(spent + amount, 2)
        remain = round(budget - new_spent, 2)
        warn = ""
        if new_spent > budget:
            warn = f"\n\n⚠️ <b>Выход за лимит</b> на <b>{round(new_spent - budget, 2)}</b>"

        bets_count = count_bets_in_shift(shift_id)

        await state.clear()
        await message.answer(
            f"✅ <b>Ставка сохранена</b>\n\n"
            f"💸 Сумма: <b>{amount}</b>\n"
            f"📊 Поставлено: <b>{new_spent}</b> / <b>{budget}</b>\n"
            f"🎯 Ставок в смене: <b>{bets_count}</b>\n"
            f"🟢 Остаток: <b>{remain}</b>{warn}",
            reply_markup=bets_menu_kb(),
        )
        return

    if not is_forward_message(message):
        return

    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("⚠️ Сначала начни смену.", reply_markup=shift_menu_kb(False))
        return

    parsed = parse_bet(text)
    if not parsed:
        await message.answer(
            "⚠️ <b>Формат ставки не распознан</b>\n\n"
            "Я не смог корректно разобрать сообщение.\n"
            "Перешли ставку ещё раз в исходном формате.",
            reply_markup=bets_menu_kb(),
        )
        log_warning("Bet parse failed")
        return

    await state.update_data(pending_bet=parsed)
    await state.set_state(ShiftState.waiting_bet_amount)

    match_start = datetime.fromisoformat(parsed["match_start_at"]).astimezone(TIMEZONE).strftime("%d.%m.%Y %H:%M")

    await message.answer(
        f"🎯 <b>Ставка распознана</b>\n\n"
        f"🏅 Спорт: <b>{parsed['sport']}</b>\n"
        f"🏆 Турнир: <b>{parsed['tournament'] or '-'}</b>\n"
        f"🏟 Матч: <b>{parsed['match_name']}</b>\n"
        f"📌 Маркет: {parsed['market']}\n"
        f"📈 КФ: <b>{parsed['odds']}</b>\n"
        f"🧠 Мат. ожидание: <b>{parsed['ev'] if parsed['ev'] is not None else '-'}</b>\n"
        f"🏦 БК: <b>{parsed['bookmaker']}</b>\n"
        f"🕒 Старт: <b>{match_start} МСК</b>\n\n"
        f"💬 Теперь напиши сумму ставки.",
        reply_markup=amount_retry_kb(),
    )


# =========================
# REMINDERS
# =========================
async def reminder_job():
    reminders = get_due_reminders()
    for item in reminders:
        dt_text = item["match_start_at"].astimezone(TIMEZONE).strftime("%d.%m.%Y %H:%M")
        try:
            await bot.send_message(
                item["user_id"],
                "⏰ <b>Напоминание</b>\n\n"
                f"Через {REMINDER_MINUTES} минут матч:\n"
                f"🏟 <b>{item['match_name']}</b>\n"
                f"📌 {item['market']}\n"
                f"💸 Сумма: <b>{item['stake']}</b>\n"
                f"📈 КФ: <b>{item['odds']}</b>\n"
                f"🏦 БК: <b>{item['bookmaker']}</b>\n"
                f"🕒 Старт: <b>{dt_text} МСК</b>",
                reply_markup=bets_menu_kb(),
            )
            mark_reminder_sent(item["id"])
            log_info(f"Reminder sent | bet_id={item['id']}")
        except Exception as e:
            log_error(f"Reminder failed | bet_id={item['id']} | error={e}")


# =========================
# STARTUP
# =========================
async def main():
    print("BOT STARTED")
    init_db()
    scheduler.add_job(reminder_job, "interval", seconds=30, max_instances=1, coalesce=True)
    scheduler.start()
    log_info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
