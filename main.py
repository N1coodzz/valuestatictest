import asyncio
import hashlib
import os
import re
import sqlite3
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
DB_PATH = "bot.db"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found in environment")
if not OWNER_ID:
    raise RuntimeError("OWNER_ID not found in environment")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())


class ShiftState(StatesGroup):
    waiting_budget = State()
    waiting_bet_amount = State()


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def as_float(text: str) -> float:
    return float(text.replace(" ", "").replace(",", "."))


def connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    db = connect()

    db.execute("""
    CREATE TABLE IF NOT EXISTS shifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        budget REAL NOT NULL,
        spent REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL
    )
    """)

    db.execute("""
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
        market TEXT,
        odds REAL,
        ev REAL,
        bookmaker TEXT,
        stake REAL,
        source_text TEXT,
        match_hash TEXT UNIQUE,
        reminder_sent INTEGER DEFAULT 0
    )
    """)

    db.commit()
    db.close()


def get_active_shift(user_id: int):
    db = connect()
    row = db.execute("""
        SELECT id, budget, spent, started_at
        FROM shifts
        WHERE user_id = ? AND status = 'active'
        ORDER BY id DESC
        LIMIT 1
    """, (user_id,)).fetchone()
    db.close()
    return row


def start_shift_db(user_id: int, started_at: str, budget: float):
    db = connect()
    db.execute("""
        INSERT INTO shifts(user_id, started_at, budget, spent, status)
        VALUES (?, ?, ?, 0, 'active')
    """, (user_id, started_at, budget))
    db.commit()
    db.close()


def end_shift_db(shift_id: int, ended_at: str):
    db = connect()
    db.execute("""
        UPDATE shifts
        SET ended_at = ?, status = 'ended'
        WHERE id = ?
    """, (ended_at, shift_id))
    db.commit()
    db.close()


def add_bet_db(shift_id: int, user_id: int, created_at: str, parsed: dict, stake: float):
    db = connect()
    db.execute("""
        INSERT INTO bets(
            shift_id, user_id, created_at,
            sport, tournament, match_name, match_date, match_time,
            market, odds, ev, bookmaker, stake, source_text, match_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        shift_id,
        user_id,
        created_at,
        parsed["sport"],
        parsed["tournament"],
        parsed["match_name"],
        parsed["match_date"],
        parsed["match_time"],
        parsed["market"],
        parsed["odds"],
        parsed["ev"],
        parsed["bookmaker"],
        stake,
        parsed["source_text"],
        parsed["hash"],
    ))

    db.execute("""
        UPDATE shifts
        SET spent = spent + ?
        WHERE id = ?
    """, (stake, shift_id))

    db.commit()
    db.close()


def get_last_bets(user_id: int, limit: int = 10):
    db = connect()
    rows = db.execute("""
        SELECT sport, match_name, market, odds, stake, bookmaker, created_at
        FROM bets
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (user_id, limit)).fetchall()
    db.close()
    return rows


def parse_bet(text: str):
    sport_line = re.search(r"⚽️🏒🎾\s*(.+?)\n", text, re.S)
    event_line = re.search(r"🚩\s*(.+?),\s*(\d{1,2}:\d{2})\s+(\d{2}/\d{2})", text)
    market_line = re.search(r"❗️\s*(.+?)\s*коэф\.?\s*([\d.,]+)❗️", text, re.S)
    ev_line = re.search(r"Математическое ожидание\s*≈\s*([\d.,]+)%", text, re.I)
    bk_line = re.search(r"Ставка сделана👉\s*([^\n(]+)", text, re.I)

    if not sport_line or not event_line or not market_line:
        return None

    full_header = sport_line.group(1).strip()
    parts = [x.strip() for x in re.split(r"\s+-\s+", full_header) if x.strip()]
    sport = parts[0] if parts else ""
    tournament = " - ".join(parts[1:]) if len(parts) > 1 else ""

    match_name = event_line.group(1).strip()
    match_time = event_line.group(2).strip()
    match_date = event_line.group(3).strip()

    market = re.sub(r"\s+", " ", market_line.group(1)).strip()
    odds = float(market_line.group(2).replace(",", "."))
    ev = float(ev_line.group(1).replace(",", ".")) if ev_line else None
    bookmaker = bk_line.group(1).strip() if bk_line else ""

    return {
        "sport": sport,
        "tournament": tournament,
        "match_name": match_name,
        "match_time": match_time,
        "match_date": match_date,
        "market": market,
        "odds": odds,
        "ev": ev,
        "bookmaker": bookmaker,
        "hash": hashlib.md5(text.strip().encode("utf-8")).hexdigest(),
        "source_text": text.strip(),
    }


def is_forward_message(message: Message) -> bool:
    return bool(
        getattr(message, "forward_origin", None)
        or getattr(message, "forward_from_chat", None)
        or getattr(message, "forward_from", None)
        or getattr(message, "forward_sender_name", None)
    )


def main_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Начать смену", callback_data="start_shift")],
            [InlineKeyboardButton(text="Текущая смена", callback_data="current_shift")],
            [InlineKeyboardButton(text="Последняя ставка", callback_data="last_bet")],
            [InlineKeyboardButton(text="Последние 10 ставок", callback_data="last_10")],
            [InlineKeyboardButton(text="Закончить смену", callback_data="end_shift")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_action")],
        ]
    )


def cancel_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_action")]
        ]
    )


@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        await message.answer("Этот бот только для владельца.")
        return

    await state.clear()
    await message.answer("Бот запущен. Выбери действие.", reply_markup=main_kb())


@dp.callback_query(F.data == "cancel_action")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Действие отменено.", reply_markup=main_kb())
    await callback.answer()


@dp.callback_query(F.data == "start_shift")
async def start_shift_cb(callback: CallbackQuery, state: FSMContext):
    active = get_active_shift(callback.from_user.id)
    if active:
        shift_id, budget, spent, started_at = active
        remain = budget - spent
        await callback.message.answer(
            f"Смена уже активна.\nБюджет: {budget}\nПоставлено: {spent}\nОстаток: {remain}",
            reply_markup=main_kb()
        )
        await callback.answer()
        return

    await state.set_state(ShiftState.waiting_budget)
    await callback.message.answer(
        "Введи бюджет смены одним числом. Например: 10000",
        reply_markup=cancel_kb()
    )
    await callback.answer()


@dp.message(ShiftState.waiting_budget)
async def budget_input(message: Message, state: FSMContext):
    try:
        budget = as_float(message.text.strip())
        if budget <= 0:
            raise ValueError
    except Exception:
        await message.answer("Нужно число. Например: 10000", reply_markup=cancel_kb())
        return

    start_shift_db(message.from_user.id, now_str(), budget)
    await state.clear()
    await message.answer(f"Смена начата.\nБюджет: {budget}", reply_markup=main_kb())


@dp.callback_query(F.data == "current_shift")
async def current_shift_cb(callback: CallbackQuery):
    active = get_active_shift(callback.from_user.id)
    if not active:
        await callback.message.answer("Активной смены нет.", reply_markup=main_kb())
        await callback.answer()
        return

    shift_id, budget, spent, started_at = active
    remain = budget - spent
    await callback.message.answer(
        f"Текущая смена:\nНачало: {started_at}\nБюджет: {budget}\nПоставлено: {spent}\nОстаток: {remain}",
        reply_markup=main_kb()
    )
    await callback.answer()


@dp.callback_query(F.data == "end_shift")
async def end_shift_cb(callback: CallbackQuery):
    active = get_active_shift(callback.from_user.id)
    if not active:
        await callback.message.answer("Активной смены нет.", reply_markup=main_kb())
        await callback.answer()
        return

    shift_id, budget, spent, started_at = active
    remain = budget - spent
    end_shift_db(shift_id, now_str())

    await callback.message.answer(
        f"Смена завершена.\nБюджет: {budget}\nПоставлено: {spent}\nОстаток: {remain}",
        reply_markup=main_kb()
    )
    await callback.answer()


@dp.callback_query(F.data == "last_10")
async def last_10_cb(callback: CallbackQuery):
    rows = get_last_bets(callback.from_user.id, 10)
    if not rows:
        await callback.message.answer("Пока нет ставок.", reply_markup=main_kb())
        await callback.answer()
        return

    lines = []
    for i, row in enumerate(rows, start=1):
        sport, match_name, market, odds, stake, bookmaker, created_at = row
        lines.append(
            f"{i}. {sport} | {match_name}\n"
            f"Маркет: {market}\n"
            f"КФ: {odds} | Сумма: {stake} | БК: {bookmaker}"
        )

    await callback.message.answer("\n\n".join(lines), reply_markup=main_kb())
    await callback.answer()


@dp.callback_query(F.data == "last_bet")
async def last_bet_cb(callback: CallbackQuery):
    rows = get_last_bets(callback.from_user.id, 1)
    if not rows:
        await callback.message.answer("Пока нет ставок.", reply_markup=main_kb())
        await callback.answer()
        return

    sport, match_name, market, odds, stake, bookmaker, created_at = rows[0]
    await callback.message.answer(
        f"Последняя ставка:\n{sport} | {match_name}\nМаркет: {market}\nКФ: {odds}\nСумма: {stake}\nБК: {bookmaker}\nДобавлена: {created_at}",
        reply_markup=main_kb()
    )
    await callback.answer()


@dp.message(F.text)
async def forwarded_bet_handler(message: Message, state: FSMContext):
    if message.from_user.id != OWNER_ID:
        return

    current_state = await state.get_state()

    if current_state == ShiftState.waiting_budget.state:
        return

    if current_state == ShiftState.waiting_bet_amount.state:
        data = await state.get_data()
        pending = data.get("pending_bet")

        if not pending:
            await state.clear()
            await message.answer("Ставка не найдена. Начни заново.", reply_markup=main_kb())
            return

        try:
            amount = as_float(message.text.strip())
            if amount <= 0:
                raise ValueError
        except Exception:
            await message.answer("Введи сумму числом. Например: 1500", reply_markup=cancel_kb())
            return

        active = get_active_shift(message.from_user.id)
        if not active:
            await state.clear()
            await message.answer("Активной смены нет.", reply_markup=main_kb())
            return

        shift_id, budget, spent, started_at = active

        try:
            add_bet_db(shift_id, message.from_user.id, now_str(), pending, amount)
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                await state.clear()
                await message.answer("Эта ставка уже была добавлена.", reply_markup=main_kb())
                return

            await state.clear()
            await message.answer(f"Ошибка записи ставки: {e}", reply_markup=main_kb())
            return

        new_spent = spent + amount
        remain = budget - new_spent
        warn = ""
        if new_spent > budget:
            warn = f"\n⚠️ Выход за лимит на {round(new_spent - budget, 2)}"

        await state.clear()
        await message.answer(
            f"✅ Ставка сохранена.\n"
            f"Сумма: {amount}\n"
            f"Поставлено: {round(new_spent, 2)} / {budget}\n"
            f"Остаток: {round(remain, 2)}{warn}",
            reply_markup=main_kb()
        )
        return

    if not is_forward_message(message):
        return

    active = get_active_shift(message.from_user.id)
    if not active:
        await message.answer("Сначала начни смену.", reply_markup=main_kb())
        return

    parsed = parse_bet(message.text.strip())
    if not parsed:
        await message.answer(
            "Не смог точно распознать ставку. Перешли сообщение ещё раз.",
            reply_markup=main_kb()
        )
        return

    await state.update_data(pending_bet=parsed)
    await state.set_state(ShiftState.waiting_bet_amount)

    await message.answer(
        f"Найдена ставка:\n"
        f"<b>{parsed['sport']}</b>\n"
        f"{parsed['tournament']}\n"
        f"{parsed['match_name']}\n"
        f"{parsed['market']}\n"
        f"КФ: {parsed['odds']}\n"
        f"Мат. ожидание: {parsed['ev'] if parsed['ev'] is not None else '-'}\n"
        f"БК: {parsed['bookmaker']}\n\n"
        f"Напиши сумму ставки.",
        reply_markup=cancel_kb()
    )


async def main():
    init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
