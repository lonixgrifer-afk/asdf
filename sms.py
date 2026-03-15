import asyncio
import hmac
import hashlib
import json
import logging
import os
from pathlib import Path  # Добавлено для работы с путями
from dataclasses import dataclass  # Добавлено для работы с @dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties # Для настройки HTML
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from aiohttp import web


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sms-bot")


def load_dotenv_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)

BOT_TOKEN=8668523159:AAFSn8gBcEG-t-zS0fqXEggnShFjK3077ck
POSTGRES_DSN=postgresql://user:password@localhost:5432/smsbot
GRIZZLY_API_KEY=d6e8983336c95b9deb8a7ec15791df6d
GRIZZLY_BASE_URL=https://api.grizzlysms.com/stubs/handler_api.php
CRYPTO_PAY_TOKEN=548485:AAfhdNPhiQU4aKSAy2prd1y78EYaSDiQdWF
CRYPTO_PAY_BASE_URL=https://pay.crypt.bot/api
CRYPTO_WEBHOOK_SECRET=change_me
CRYPTO_WEBHOOK_HOST=0.0.0.0
CRYPTO_WEBHOOK_PORT=8081
CRYPTO_WEBHOOK_PATH=/cryptobot/webhook
MIN_TOPUP_AMOUNT=1
REFERRAL_PERCENT=5
VIP_THRESHOLD_TOTAL_SPENT=500
VIP_DISCOUNT_PERCENT=10
OWNER_CHAT_ID=0
REVIEW_GROUP_ID=0


@dataclass(slots=True)
class Config:
    bot_token: str
    postgres_dsn: str
    grizzly_api_key: str
    grizzly_base_url: str
    crypto_pay_token: str
    crypto_pay_base_url: str
    crypto_webhook_secret: str
    crypto_webhook_host: str
    crypto_webhook_port: int
    crypto_webhook_path: str
    min_topup_amount: Decimal
    referral_percent: Decimal
    vip_threshold_total_spent: Decimal
    vip_discount_percent: Decimal
    owner_chat_id: int
    review_group_id: int

    @staticmethod
    def from_env() -> "Config":
        # Если переменной нет в системе, будет использовано значение из кавычек
        return Config(
            bot_token=os.getenv("BOT_TOKEN", "ТВОЙ_ТОКЕН_БОТА"),
            postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://user:pass@localhost:5432/dbname"),
            grizzly_api_key=os.getenv("GRIZZLY_API_KEY", "ТВОЙ_КЛЮЧ_GRIZZLY"),
            grizzly_base_url="https://api.grizzlysms.com/stubs/handler_api.php",
            crypto_pay_token=os.getenv("CRYPTO_PAY_TOKEN", "ТВОЙ_ТОКЕН_CRYPTOPAY"),
            crypto_pay_base_url="https://pay.crypt.bot/api",
            crypto_webhook_secret="change_me",
            crypto_webhook_host="0.0.0.0",
            crypto_webhook_port=8081,
            crypto_webhook_path="/cryptobot/webhook",
            min_topup_amount=Decimal("1"),
            referral_percent=Decimal("5"),
            vip_threshold_total_spent=Decimal("500"),
            vip_discount_percent=Decimal("10"),
            owner_chat_id=0,
            review_group_id=0,
        )


class DB:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def migrate(self) -> None:
        async with self.pool.acquire() as con:
            await con.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    balance NUMERIC(18,2) NOT NULL DEFAULT 0,
                    referrer_id BIGINT,
                    total_spent NUMERIC(18,2) NOT NULL DEFAULT 0,
                    vip_until TIMESTAMPTZ,
                    low_balance_threshold NUMERIC(18,2) DEFAULT 0,
                    is_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS services (
                    code TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    markup_percent NUMERIC(8,2) NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS countries (
                    code TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    is_enabled BOOLEAN NOT NULL DEFAULT TRUE
                );

                CREATE TABLE IF NOT EXISTS favorites (
                    user_id BIGINT NOT NULL,
                    service_code TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, service_code, country_code)
                );

                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    discount_percent NUMERIC(8,2),
                    discount_amount NUMERIC(18,2),
                    max_uses INTEGER,
                    used_count INTEGER NOT NULL DEFAULT 0,
                    expires_at TIMESTAMPTZ,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE
                );

                CREATE TABLE IF NOT EXISTS promo_code_usages (
                    code TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (code, user_id)
                );

                CREATE TABLE IF NOT EXISTS topups (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    invoice_id BIGINT,
                    amount NUMERIC(18,2) NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'USDT',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    paid_at TIMESTAMPTZ
                );

                CREATE TABLE IF NOT EXISTS orders (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    service_code TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    provider_order_id TEXT,
                    phone_number TEXT,
                    status TEXT NOT NULL,
                    provider_cost NUMERIC(18,4),
                    client_price NUMERIC(18,2) NOT NULL,
                    promo_code TEXT,
                    sms_code TEXT,
                    sms_count INTEGER NOT NULL DEFAULT 0,
                    reserved_until TIMESTAMPTZ,
                    is_favorite_saved BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    amount NUMERIC(18,2) NOT NULL,
                    kind TEXT NOT NULL,
                    reference TEXT,
                    details JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS reviews (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    order_id BIGINT NOT NULL,
                    rating INTEGER NOT NULL,
                    comment TEXT,
                    is_public BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS faq_items (
                    id BIGSERIAL PRIMARY KEY,
                    category TEXT NOT NULL,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 100
                );

                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            await con.execute(
                """
                INSERT INTO services(code, title) VALUES
                    ('go', 'Google'),
                    ('tg', 'Telegram'),
                    ('ig', 'Instagram'),
                    ('tt', 'TikTok'),
                    ('fb', 'Facebook')
                ON CONFLICT (code) DO NOTHING;
                """
            )
            await con.execute(
                """
                INSERT INTO countries(code, title) VALUES
                    ('ru', 'Россия'),
                    ('kz', 'Казахстан'),
                    ('ua', 'Украина'),
                    ('pl', 'Польша'),
                    ('us', 'США')
                ON CONFLICT (code) DO NOTHING;
                """
            )

    async def upsert_user(self, user_id: int, username: Optional[str], first_name: Optional[str], referrer_id: Optional[int]) -> None:
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO users(id, username, first_name, referrer_id)
                VALUES($1, $2, $3, $4)
                ON CONFLICT (id)
                DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name, updated_at=NOW()
                """,
                user_id,
                username,
                first_name,
                referrer_id if referrer_id != user_id else None,
            )

    async def user(self, user_id: int) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM users WHERE id=$1", user_id)

    async def enabled_services(self):
        async with self.pool.acquire() as con:
            return await con.fetch("SELECT code, title FROM services WHERE is_enabled=TRUE ORDER BY title")

    async def enabled_countries(self):
        async with self.pool.acquire() as con:
            return await con.fetch("SELECT code, title FROM countries WHERE is_enabled=TRUE ORDER BY title")

    async def set_balance(self, user_id: int, amount: Decimal) -> None:
        async with self.pool.acquire() as con:
            await con.execute("UPDATE users SET balance=$2, updated_at=NOW() WHERE id=$1", user_id, amount)

    async def add_balance(self, user_id: int, amount: Decimal, kind: str, reference: str, details: dict[str, Any]) -> None:
        async with self.pool.acquire() as con:
            async with con.transaction():
                await con.execute("UPDATE users SET balance=balance+$2, updated_at=NOW() WHERE id=$1", user_id, amount)
                await con.execute(
                    "INSERT INTO transactions(user_id, amount, kind, reference, details) VALUES($1, $2, $3, $4, $5::jsonb)",
                    user_id,
                    amount,
                    kind,
                    reference,
                    json.dumps(details),
                )

    async def reserve_order(self, user_id: int, service_code: str, country_code: str, price: Decimal, promo_code: Optional[str]) -> Optional[int]:
        async with self.pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow("SELECT balance FROM users WHERE id=$1 FOR UPDATE", user_id)
                if row is None or Decimal(str(row["balance"])) < price:
                    return None
                await con.execute("UPDATE users SET balance=balance-$2, updated_at=NOW() WHERE id=$1", user_id, price)
                rec = await con.fetchrow(
                    """
                    INSERT INTO orders(user_id, service_code, country_code, status, client_price, promo_code, reserved_until)
                    VALUES($1, $2, $3, 'reserved', $4, $5, NOW() + INTERVAL '20 minutes')
                    RETURNING id
                    """,
                    user_id,
                    service_code,
                    country_code,
                    price,
                    promo_code,
                )
                await con.execute(
                    "INSERT INTO transactions(user_id, amount, kind, reference, details) VALUES($1, $2, 'reserve', $3, $4::jsonb)",
                    user_id,
                    -price,
                    f"order:{rec['id']}",
                    json.dumps({"service": service_code, "country": country_code}),
                )
                return int(rec["id"])

    async def set_order_provider(self, order_id: int, provider_order_id: str, phone_number: str, provider_cost: Decimal):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE orders SET provider_order_id=$2, phone_number=$3, provider_cost=$4, status='active' WHERE id=$1",
                order_id,
                provider_order_id,
                phone_number,
                provider_cost,
            )

    async def complete_order(self, order_id: int, sms_code: str) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow("SELECT * FROM orders WHERE id=$1 FOR UPDATE", order_id)
                if not row or row["status"] not in {"active", "reserved"}:
                    return None
                await con.execute(
                    "UPDATE orders SET status='completed', sms_code=$2, sms_count=sms_count+1, completed_at=NOW() WHERE id=$1",
                    order_id,
                    sms_code,
                )
                await con.execute(
                    "UPDATE users SET total_spent=total_spent+$2, updated_at=NOW() WHERE id=$1",
                    row["user_id"],
                    row["client_price"],
                )
                await con.execute(
                    "INSERT INTO transactions(user_id, amount, kind, reference, details) VALUES($1, 0, 'purchase_done', $2, $3::jsonb)",
                    row["user_id"],
                    f"order:{order_id}",
                    json.dumps({"sms_code": sms_code}),
                )
                return row

    async def cancel_order(self, order_id: int, reason: str = "cancel") -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow("SELECT * FROM orders WHERE id=$1 FOR UPDATE", order_id)
                if not row or row["status"] in {"completed", "cancelled", "timeout"}:
                    return None
                await con.execute("UPDATE orders SET status=$2 WHERE id=$1", order_id, "cancelled" if reason == "cancel" else "timeout")
                await con.execute("UPDATE users SET balance=balance+$2, updated_at=NOW() WHERE id=$1", row["user_id"], row["client_price"])
                await con.execute(
                    "INSERT INTO transactions(user_id, amount, kind, reference, details) VALUES($1, $2, 'refund', $3, $4::jsonb)",
                    row["user_id"],
                    row["client_price"],
                    f"order:{order_id}",
                    json.dumps({"reason": reason}),
                )
                return row

    async def active_order(self, user_id: int) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetchrow(
                "SELECT * FROM orders WHERE user_id=$1 AND status IN ('reserved','active') ORDER BY id DESC LIMIT 1",
                user_id,
            )

    async def order(self, order_id: int) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)

    async def recent_orders(self, user_id: int, limit: int = 10):
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT id, service_code, country_code, status, client_price, sms_code, created_at FROM orders WHERE user_id=$1 ORDER BY id DESC LIMIT $2",
                user_id,
                limit,
            )

    async def create_topup(self, user_id: int, amount: Decimal, currency: str) -> int:
        async with self.pool.acquire() as con:
            rec = await con.fetchrow(
                "INSERT INTO topups(user_id, amount, currency) VALUES($1, $2, $3) RETURNING id",
                user_id,
                amount,
                currency,
            )
            return int(rec["id"])

    async def bind_invoice(self, topup_id: int, invoice_id: int):
        async with self.pool.acquire() as con:
            await con.execute("UPDATE topups SET invoice_id=$2 WHERE id=$1", topup_id, invoice_id)

    async def credit_paid_topup(self, invoice_id: int) -> Optional[tuple[int, Decimal, int]]:
        async with self.pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow("SELECT * FROM topups WHERE invoice_id=$1 FOR UPDATE", invoice_id)
                if not row or row["status"] == "paid":
                    return None
                await con.execute("UPDATE topups SET status='paid', paid_at=NOW() WHERE id=$1", row["id"])
                await con.execute("UPDATE users SET balance=balance+$2, updated_at=NOW() WHERE id=$1", row["user_id"], row["amount"])
                await con.execute(
                    "INSERT INTO transactions(user_id, amount, kind, reference, details) VALUES($1, $2, 'topup', $3, $4::jsonb)",
                    row["user_id"],
                    row["amount"],
                    f"invoice:{invoice_id}",
                    json.dumps({"topup_id": row["id"], "currency": row["currency"]}),
                )
                return int(row["user_id"]), Decimal(str(row["amount"])), int(row["id"])

    async def calculate_price(self, service_code: str, provider_price: Decimal, user_id: int) -> Decimal:
        async with self.pool.acquire() as con:
            service = await con.fetchrow("SELECT markup_percent FROM services WHERE code=$1", service_code)
            user = await con.fetchrow("SELECT total_spent FROM users WHERE id=$1", user_id)
            markup = Decimal(str(service["markup_percent"] if service else 0))
            total = provider_price * (Decimal("1") + markup / Decimal("100"))
            if user and Decimal(str(user["total_spent"])) >= get_settings().vip_threshold_total_spent:
                total = total * (Decimal("1") - get_settings().vip_discount_percent / Decimal("100"))
            return total.quantize(Decimal("0.01"))


class GrizzlyClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url

    async def _request(self, params: dict[str, Any]) -> str:
        query = {"api_key": self.api_key, **params}
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url, params=query, timeout=20) as resp:
                body = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"Grizzly HTTP {resp.status}: {body}")
                return body

    async def get_price(self, service: str, country: str) -> Decimal:
        response = await self._request({"action": "getPrices", "service": service, "country": country})
        try:
            data = json.loads(response)
            value = data[country][service]["cost"]
        except Exception as exc:
            raise RuntimeError(f"Cannot parse getPrices response: {response}") from exc
        return Decimal(str(value))

    async def buy_number(self, service: str, country: str) -> tuple[str, str]:
        response = await self._request({"action": "getNumber", "service": service, "country": country})
        # Example ACCESS_NUMBER:123456:79990000000
        if not response.startswith("ACCESS_NUMBER"):
            raise RuntimeError(f"Grizzly buy failed: {response}")
        _, activation_id, number = response.split(":", 2)
        return activation_id, number

    async def get_status(self, activation_id: str) -> tuple[str, Optional[str]]:
        response = await self._request({"action": "getStatus", "id": activation_id})
        if response.startswith("STATUS_OK"):
            parts = response.split(":", 1)
            return "ok", parts[1] if len(parts) > 1 else None
        if response == "STATUS_WAIT_CODE":
            return "wait", None
        if response in {"STATUS_CANCEL", "STATUS_FINISH"}:
            return "closed", None
        return "unknown", response

    async def set_status(self, activation_id: str, status: int) -> str:
        return await self._request({"action": "setStatus", "id": activation_id, "status": status})


class CryptoPayClient:
    def __init__(self, token: str, base_url: str):
        self.token = token
        self.base_url = base_url.rstrip("/")

    async def create_invoice(self, amount: Decimal, asset: str, payload: str, description: str) -> dict[str, Any]:
        url = f"{self.base_url}/createInvoice"
        headers = {"Crypto-Pay-API-Token": self.token}
        data = {
            "asset": asset,
            "amount": str(amount),
            "description": description,
            "payload": payload,
            "allow_comments": False,
            "allow_anonymous": False,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data, timeout=20) as resp:
                body = await resp.text()
                parsed = json.loads(body)
                if resp.status >= 400 or not parsed.get("ok"):
                    raise RuntimeError(f"CryptoPay createInvoice failed: {body}")
                return parsed["result"]


class BuyState(StatesGroup):
    waiting_service = State()
    waiting_country = State()


class TopupState(StatesGroup):
    waiting_amount = State()


SETTINGS: Optional[Config] = None
bot: Optional[Bot] = None
dp = Dispatcher()
pool: asyncpg.Pool
db: DB
grizzly: GrizzlyClient
crypto: CryptoPayClient
active_polling_tasks: dict[int, asyncio.Task] = {}


def get_settings() -> Config:
    global SETTINGS
    if SETTINGS is None:
        load_dotenv_file()
        SETTINGS = Config.from_env()
    return SETTINGS


def get_bot() -> Bot:
    if bot is None:
        raise RuntimeError("Bot is not initialized. Run main() first.")
    return bot


MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📱 Купить номер"), KeyboardButton(text="💰 Баланс")],
        [KeyboardButton(text="➕ Пополнить баланс"), KeyboardButton(text="⭐ Избранное")],
        [KeyboardButton(text="📦 История заказов"), KeyboardButton(text="🎁 Промокод")],
        [KeyboardButton(text="👥 Реферальная программа"), KeyboardButton(text="❓ FAQ")],
        [KeyboardButton(text="📞 Поддержка")],
    ],
    resize_keyboard=True,
)


async def ensure_user(message: Message) -> None:
    referrer_id = None
    if message.text and message.text.startswith("/start "):
        payload = message.text.split(maxsplit=1)[1]
        if payload.startswith("ref_"):
            try:
                referrer_id = int(payload.replace("ref_", "", 1))
            except ValueError:
                referrer_id = None
    await db.upsert_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        referrer_id=referrer_id,
    )


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_user(message)
    me = await db.user(message.from_user.id)
    await message.answer(
        "Добро пожаловать в SMS-магазин.\n"
        f"Ваш баланс: <b>{Decimal(str(me['balance'])):.2f}</b> USDT",
        reply_markup=MAIN_KB,
    )


@dp.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    await ensure_user(message)
    me = await db.user(message.from_user.id)
    balance = Decimal(str(me["balance"]))
    await message.answer(
        "<b>Ваш баланс</b>\n"
        f"Текущий остаток: <b>{balance:.2f} USDT</b>\n"
        f"Порог уведомления: {Decimal(str(me['low_balance_threshold'])):.2f} USDT\n\n"
        f"Купить можно примерно: {int(balance // Decimal('0.15'))} номеров (при цене 0.15)",
    )


@dp.message(F.text == "📱 Купить номер")
async def buy_start(message: Message, state: FSMContext):
    await ensure_user(message)
    services = await db.enabled_services()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=s["title"], callback_data=f"svc:{s['code']}")] for s in services]
    )
    await state.set_state(BuyState.waiting_service)
    await message.answer("Выберите сервис:", reply_markup=kb)


@dp.callback_query(BuyState.waiting_service, F.data.startswith("svc:"))
async def buy_pick_service(cb: CallbackQuery, state: FSMContext):
    service_code = cb.data.split(":", 1)[1]
    countries = await db.enabled_countries()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=c["title"], callback_data=f"cnt:{service_code}:{c['code']}")] for c in countries]
    )
    await state.set_state(BuyState.waiting_country)
    await state.update_data(service_code=service_code)
    await cb.message.edit_text("Выберите страну:", reply_markup=kb)
    await cb.answer()


@dp.callback_query(BuyState.waiting_country, F.data.startswith("cnt:"))
async def buy_pick_country(cb: CallbackQuery, state: FSMContext):
    _, service_code, country_code = cb.data.split(":", 2)
    user_id = cb.from_user.id

    provider_price = await grizzly.get_price(service_code, country_code)
    final_price = await db.calculate_price(service_code, provider_price, user_id)

    order_id = await db.reserve_order(user_id, service_code, country_code, final_price, promo_code=None)
    if not order_id:
        await cb.message.edit_text(
            f"Недостаточно средств. Цена: <b>{final_price:.2f} USDT</b>\nПополните баланс.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="➕ Пополнить", callback_data="topup:start")]]
            ),
        )
        await state.clear()
        await cb.answer()
        return

    try:
        activation_id, number = await grizzly.buy_number(service_code, country_code)
        await db.set_order_provider(order_id, activation_id, number, provider_price)
    except Exception as exc:
        await db.cancel_order(order_id, reason="provider_error")
        await cb.message.edit_text(f"Ошибка при выдаче номера: {exc}. Средства возвращены.")
        await state.clear()
        await cb.answer()
        return

    task = asyncio.create_task(poll_sms(order_id, user_id))
    active_polling_tasks[order_id] = task

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Получить код ещё раз", callback_data=f"sms:again:{order_id}")],
            [InlineKeyboardButton(text="❌ Отменить номер", callback_data=f"sms:cancel:{order_id}")],
            [InlineKeyboardButton(text="🔂 Повторить заказ", callback_data=f"sms:repeat:{service_code}:{country_code}")],
        ]
    )
    await cb.message.edit_text(
        "Номер успешно выдан.\n"
        f"<b>Номер:</b> <code>{number}</code>\n"
        "Ожидаю SMS-код (до 20 минут)...",
        reply_markup=kb,
    )
    await state.clear()
    await cb.answer()


async def poll_sms(order_id: int, user_id: int):
    deadline = datetime.now(timezone.utc) + timedelta(minutes=20)
    while datetime.now(timezone.utc) < deadline:
        order = await db.order(order_id)
        if not order or order["status"] not in {"active", "reserved"}:
            return
        try:
            status, payload = await grizzly.get_status(order["provider_order_id"])
            if status == "ok" and payload:
                await db.complete_order(order_id, payload)
                await get_bot().send_message(
                    user_id,
                    "✅ SMS получен!\n"
                    f"Код: <code>{payload}</code>\n"
                    f"Заказ #{order_id} закрыт.",
                )
                await ask_review(user_id, order_id)
                return
            if status == "closed":
                await db.cancel_order(order_id, reason="provider_closed")
                await get_bot().send_message(user_id, "Поставщик закрыл активацию. Средства возвращены.")
                return
        except Exception as exc:
            logger.exception("Polling failed for order=%s: %s", order_id, exc)
        await asyncio.sleep(7)

    order = await db.order(order_id)
    if order and order["status"] in {"active", "reserved"}:
        try:
            await grizzly.set_status(order["provider_order_id"], 8)
        except Exception:
            logger.warning("Failed to cancel provider activation order=%s", order_id)
        await db.cancel_order(order_id, reason="timeout")
        await get_bot().send_message(user_id, "⏱ Время ожидания SMS истекло. Средства возвращены на баланс.")


@dp.callback_query(F.data.startswith("sms:cancel:"))
async def cancel_number(cb: CallbackQuery):
    order_id = int(cb.data.split(":", 2)[2])
    order = await db.order(order_id)
    if not order or order["user_id"] != cb.from_user.id:
        await cb.answer("Заказ не найден", show_alert=True)
        return
    if order["status"] not in {"reserved", "active"}:
        await cb.answer("Нельзя отменить", show_alert=True)
        return

    await db.cancel_order(order_id, reason="cancel")
    try:
        if order["provider_order_id"]:
            await grizzly.set_status(order["provider_order_id"], 8)
    except Exception:
        pass
    await cb.message.answer("Номер отменен, средства возвращены.")
    await cb.answer("Отменено")


@dp.callback_query(F.data.startswith("sms:again:"))
async def get_code_again(cb: CallbackQuery):
    order_id = int(cb.data.split(":", 2)[2])
    order = await db.order(order_id)
    if not order or order["user_id"] != cb.from_user.id:
        await cb.answer("Заказ не найден", show_alert=True)
        return
    if order["sms_code"]:
        await cb.answer(f"Последний код: {order['sms_code']}", show_alert=True)
    else:
        await cb.answer("Код пока не получен", show_alert=True)


@dp.callback_query(F.data.startswith("sms:repeat:"))
async def repeat_order(cb: CallbackQuery, state: FSMContext):
    _, _, service_code, country_code = cb.data.split(":", 3)
    await state.set_state(BuyState.waiting_country)
    fake_cb = CallbackQuery(
        id=cb.id,
        from_user=cb.from_user,
        chat_instance=cb.chat_instance,
        message=cb.message,
        data=f"cnt:{service_code}:{country_code}",
    )
    await buy_pick_country(fake_cb, state)


@dp.message(F.text == "📦 История заказов")
async def history(message: Message):
    rows = await db.recent_orders(message.from_user.id)
    if not rows:
        await message.answer("История пока пуста.")
        return
    lines = ["<b>Последние заказы:</b>"]
    for r in rows:
        lines.append(
            f"#{r['id']} | {r['service_code'].upper()}/{r['country_code'].upper()} | {r['status']} | {Decimal(str(r['client_price'])):.2f}"
        )
    await message.answer("\n".join(lines))


@dp.message(F.text == "➕ Пополнить баланс")
async def topup_start(message: Message, state: FSMContext):
    await state.set_state(TopupState.waiting_amount)
    await message.answer(f"Введите сумму пополнения (минимум {get_settings().min_topup_amount} USDT):")


@dp.callback_query(F.data == "topup:start")
async def topup_start_cb(cb: CallbackQuery, state: FSMContext):
    await state.set_state(TopupState.waiting_amount)
    await cb.message.answer(f"Введите сумму пополнения (минимум {get_settings().min_topup_amount} USDT):")
    await cb.answer()


@dp.message(TopupState.waiting_amount)
async def topup_amount(message: Message, state: FSMContext):
    try:
        amount = Decimal(message.text.replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        await message.answer("Некорректная сумма.")
        return
    if amount < get_settings().min_topup_amount:
        await message.answer(f"Минимальная сумма: {get_settings().min_topup_amount} USDT")
        return

    topup_id = await db.create_topup(message.from_user.id, amount, "USDT")
    invoice = await crypto.create_invoice(
        amount=amount,
        asset="USDT",
        payload=f"topup:{topup_id}:{message.from_user.id}",
        description=f"Пополнение баланса #{topup_id}",
    )
    await db.bind_invoice(topup_id, int(invoice["invoice_id"]))

    await message.answer(
        "Счет создан. Перейдите по ссылке для оплаты:\n"
        f"{invoice['pay_url']}\n\n"
        "После оплаты баланс будет зачислен автоматически.",
        disable_web_page_preview=True,
    )
    await state.clear()


@dp.message(F.text == "👥 Реферальная программа")
async def referral(message: Message):
    link = f"https://t.me/{(await get_bot().get_me()).username}?start=ref_{message.from_user.id}"
    await message.answer(
        "<b>Реферальная программа</b>\n"
        f"Ваш процент: {get_settings().referral_percent}% от каждого пополнения реферала.\n"
        f"Ссылка: {link}"
    )


@dp.message(F.text == "❓ FAQ")
async def faq(message: Message):
    await message.answer(
        "<b>FAQ</b>\n"
        "• Почему не пришло SMS? — Иногда сервис не отправляет код, в таком случае после таймаута деньги возвращаются.\n"
        "• Как пополнить баланс? — Через кнопку '➕ Пополнить баланс'.\n"
        "• Что такое VIP? — При достижении оборота включается скидка на все номера."
    )


@dp.message(F.text == "📞 Поддержка")
async def support(message: Message):
    await message.answer("Напишите ваш вопрос следующим сообщением с префиксом: SUPPORT: ...")


@dp.message(F.text.startswith("SUPPORT:"))
async def support_message(message: Message):
    text = message.text.replace("SUPPORT:", "", 1).strip()
    if get_settings().owner_chat_id:
        await get_bot().send_message(
            get_settings().owner_chat_id,
            f"🆘 Обращение в поддержку\nОт: @{message.from_user.username} ({message.from_user.id})\n\n{text}",
        )
    await message.answer("Ваше сообщение отправлено в поддержку.")


@dp.message(F.text == "⭐ Избранное")
async def favorites(message: Message):
    await message.answer("Избранное будет автоматически дополняться после успешных заказов (MVP-поведение).")


@dp.message(F.text == "🎁 Промокод")
async def promo(message: Message):
    await message.answer("Отправьте промокод сообщением вида: PROMO: CODE")


@dp.message(F.text.startswith("PROMO:"))
async def apply_promo(message: Message):
    code = message.text.replace("PROMO:", "", 1).strip().upper()
    await message.answer(f"Промокод {code} сохранен для следующего заказа (полную механику расширьте в админ-панели).")


async def ask_review(user_id: int, order_id: int):
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⭐️1", callback_data=f"review:{order_id}:1"), InlineKeyboardButton(text="⭐️2", callback_data=f"review:{order_id}:2")],
            [InlineKeyboardButton(text="⭐️3", callback_data=f"review:{order_id}:3"), InlineKeyboardButton(text="⭐️4", callback_data=f"review:{order_id}:4")],
            [InlineKeyboardButton(text="⭐️5", callback_data=f"review:{order_id}:5")],
        ]
    )
    await get_bot().send_message(user_id, "Оцените заказ:", reply_markup=kb)


@dp.callback_query(F.data.startswith("review:"))
async def review_rate(cb: CallbackQuery):
    _, order_id, stars = cb.data.split(":")
    await cb.message.answer(f"Спасибо! Оценка {stars}/5 принята. Напишите комментарий: REVIEW:{order_id}:ваш текст")
    await cb.answer("Оценка сохранена")


@dp.message(F.text.startswith("REVIEW:"))
async def review_text(message: Message):
    try:
        _, order_id, comment = message.text.split(":", 2)
        oid = int(order_id)
    except Exception:
        await message.answer("Неверный формат. Пример: REVIEW:123:Все отлично")
        return
    if get_settings().review_group_id:
        await get_bot().send_message(
            get_settings().review_group_id,
            f"📝 Новый отзыв\nUser: @{message.from_user.username} ({message.from_user.id})\n"
            f"Order: #{oid}\nКомментарий: {comment}\nДата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )
    await message.answer("Отзыв отправлен. Спасибо!")


def verify_webhook_signature(body: bytes, signature: str, secret: str) -> bool:
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, signature)


async def cryptobot_webhook(request: web.Request) -> web.Response:
    body = await request.read()
    signature = request.headers.get("X-Crypto-Pay-Signature", "")
    if not verify_webhook_signature(body, signature, get_settings().crypto_webhook_secret):
        return web.Response(status=403, text="forbidden")

    payload = json.loads(body.decode())
    update = payload.get("update", {})
    if update.get("update_type") != "invoice_paid":
        return web.json_response({"ok": True})

    invoice = update.get("payload", {}).get("invoice", {})
    invoice_id = int(invoice.get("invoice_id"))
    credited = await db.credit_paid_topup(invoice_id)
    if credited:
        user_id, amount, topup_id = credited
        await get_bot().send_message(user_id, f"✅ Пополнение #{topup_id} подтверждено. Зачислено {amount:.2f} USDT.")
        if get_settings().owner_chat_id and amount >= Decimal("100"):
            await get_bot().send_message(get_settings().owner_chat_id, f"💸 Крупное пополнение: user={user_id}, amount={amount:.2f} USDT")

    return web.json_response({"ok": True})


async def watchdog_grizzly_balance():
    while True:
        try:
            bal = await grizzly._request({"action": "getBalance"})
            if bal.startswith("ACCESS_BALANCE"):
                value = Decimal(bal.split(":", 1)[1])
                if get_settings().owner_chat_id and value < Decimal("10"):
                    await get_bot().send_message(get_settings().owner_chat_id, f"⚠️ Баланс GrizzlySMS низкий: {value}")
        except Exception as exc:
            logger.exception("Grizzly watchdog error: %s", exc)
            if get_settings().owner_chat_id:
                await get_bot().send_message(get_settings().owner_chat_id, f"❌ Ошибка Grizzly API: {exc}")
        await asyncio.sleep(300)


async def start_webhook_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_post(get_settings().crypto_webhook_path, cryptobot_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, get_settings().crypto_webhook_host, get_settings().crypto_webhook_port)
    await site.start()
    logger.info("Crypto webhook listening on %s:%s%s", get_settings().crypto_webhook_host, get_settings().crypto_webhook_port, get_settings().crypto_webhook_path)
    return runner


def maybe_print_env_example() -> bool:
    if "--env-example" in os.sys.argv:
        print(ENV_EXAMPLE, end="")
        return True
    return False



async def main():
    global pool, db, grizzly, crypto, bot
    cfg = get_settings()
    bot = Bot(cfg.bot_token, parse_mode=ParseMode.HTML)
    pool = await asyncpg.create_pool(dsn=cfg.postgres_dsn, min_size=1, max_size=10)
    db = DB(pool)
    await db.migrate()

    grizzly = GrizzlyClient(cfg.grizzly_api_key, cfg.grizzly_base_url)
    crypto = CryptoPayClient(cfg.crypto_pay_token, cfg.crypto_pay_base_url)

    runner = await start_webhook_server()
    watchdog_task = asyncio.create_task(watchdog_grizzly_balance())

    try:
        await dp.start_polling(get_bot())
    finally:
        watchdog_task.cancel()
        await runner.cleanup()
        await pool.close()
        await get_bot().session.close()

async def main():
    global pool, db, grizzly, crypto, bot
    
    # Инициализация конфига
    config = get_settings()
    
    # Инициализация бота с поддержкой HTML по умолчанию
    bot = Bot(
        token=config.bot_token, 
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    # Подключение к базе данных
    try:
        pool = await asyncpg.create_pool(dsn=config.postgres_dsn)
        db = DB(pool)
        await db.migrate() # Создание таблиц
        logger.info("База данных подключена и миграции применены")
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return

    # Инициализация API клиентов
    grizzly = GrizzlyClient(config.grizzly_api_key, config.grizzly_base_url)
    crypto = CryptoPayClient(config.crypto_pay_token, config.crypto_pay_base_url)

    logger.info("Бот запускается...")
    
    try:
        # Удаляем вебхук (если был) и запускаем Long Polling
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await pool.close()
        await (await bot.get_session()).close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    if not maybe_print_env_example():
        asyncio.run(main())
