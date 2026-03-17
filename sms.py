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
from urllib.parse import urlparse, urlunparse
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
def build_db_error_hint(dsn: str, exc: Exception) -> str:
    parsed = urlparse(dsn)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    db_name = (parsed.path or "/").lstrip("/") or "postgres"
    return (
        f"Ошибка БД: {exc}. Проверьте доступность PostgreSQL на {host}:{port}, "
        f"наличие базы '{db_name}', корректность логина/пароля в POSTGRES_DSN и "
        "разрешение входящих подключений в postgresql.conf/pg_hba.conf."
    )
def maintenance_dsn(dsn: str, db_name: str = "postgres") -> str:
    parsed = urlparse(dsn)
    new_path = f"/{db_name}"
    return urlunparse((parsed.scheme, parsed.netloc, new_path, parsed.params, parsed.query, parsed.fragment))
async def ensure_database_exists(dsn: str) -> None:
    parsed = urlparse(dsn)
    db_name = (parsed.path or "/").lstrip("/") or "postgres"
    admin_conn = await asyncpg.connect(maintenance_dsn(dsn, "postgres"))
    try:
        exists = await admin_conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", db_name)
        if not exists:
            if not db_name.replace("_", "").isalnum():
                raise RuntimeError(f"Unsafe database name: {db_name}")
            await admin_conn.execute(f'CREATE DATABASE "{db_name}"')
            logger.info("Создана база данных %s", db_name)
    finally:
        await admin_conn.close()
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
    support_username: str
    admin_user_ids: str
    @staticmethod
    def from_env() -> "Config":
        return Config(
            bot_token=os.getenv("BOT_TOKEN", "8668523159:AAFSn8gBcEG-t-zS0fqXEggnShFjK3077ck"),
            postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://postgres:qwertyuiop@localhost:5432/sms_db"),
            grizzly_api_key=os.getenv("GRIZZLY_API_KEY", "d6e8983336c95b9deb8a7ec15791df6d"),
            grizzly_base_url=os.getenv("GRIZZLY_BASE_URL", "https://api.grizzlysms.com/stubs/handler_api.php"),
            crypto_pay_token=os.getenv("CRYPTO_PAY_TOKEN", "548485:AAfhdNPhiQU4aKSAy2prd1y78EYaSDiQdWF"),
            crypto_pay_base_url=os.getenv("CRYPTO_PAY_BASE_URL", "https://pay.crypt.bot/api"),
            crypto_webhook_secret=os.getenv("CRYPTO_WEBHOOK_SECRET", "change_me"),
            crypto_webhook_host=os.getenv("CRYPTO_WEBHOOK_HOST", "0.0.0.0"),
            crypto_webhook_port=int(os.getenv("CRYPTO_WEBHOOK_PORT", "8081")),
            crypto_webhook_path=os.getenv("CRYPTO_WEBHOOK_PATH", "/cryptobot/webhook"),
            min_topup_amount=Decimal(os.getenv("MIN_TOPUP_AMOUNT", "1")),
            referral_percent=Decimal(os.getenv("REFERRAL_PERCENT", "5")),
            vip_threshold_total_spent=Decimal(os.getenv("VIP_THRESHOLD_TOTAL_SPENT", "500")),
            vip_discount_percent=Decimal(os.getenv("VIP_DISCOUNT_PERCENT", "10")),
            owner_chat_id=int(os.getenv("OWNER_CHAT_ID", "0")),
            review_group_id=int(os.getenv("REVIEW_GROUP_ID", "0")),
            support_username=os.getenv("SUPPORT_USERNAME", "@Genolay"),
            admin_user_ids=os.getenv("ADMIN_USER_IDS", "7487852172"),
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
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS is_subscription_verified BOOLEAN NOT NULL DEFAULT FALSE;
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
    async def is_subscription_verified(self, user_id: int) -> bool:
        async with self.pool.acquire() as con:
            return bool(await con.fetchval("SELECT is_subscription_verified FROM users WHERE id=$1", user_id))
    async def mark_subscription_verified(self, user_id: int) -> None:
        async with self.pool.acquire() as con:
            await con.execute("UPDATE users SET is_subscription_verified=TRUE, updated_at=NOW() WHERE id=$1", user_id)
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
    async def recent_orders(self, user_id: int, limit: int = 1000):
        async with self.pool.acquire() as con:
            return await con.fetch(
                "SELECT id, service_code, country_code, status, client_price, sms_code, created_at FROM orders WHERE user_id=$1 AND status='completed' ORDER BY id DESC LIMIT $2",
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
    async def admin_overall_stats(self) -> asyncpg.Record:
        async with self.pool.acquire() as con:
            return await con.fetchrow(
                """
                SELECT
                    (SELECT COUNT(*) FROM users) AS users_total,
                    (SELECT COUNT(*) FROM orders) AS orders_total,
                    (SELECT COUNT(*) FROM orders WHERE status = 'completed') AS completed_total,
                    (SELECT COUNT(*) FROM orders WHERE status IN ('active','reserved')) AS in_progress_total,
                    (SELECT COALESCE(SUM(balance), 0) FROM users) AS users_balance_total,
                    (SELECT COALESCE(SUM(amount), 0) FROM topups WHERE status='paid') AS topup_sum_total
                """
            )
    async def create_promo_code(
        self,
        code: str,
        bonus_type: str,
        bonus_value: Decimal,
        max_uses: int,
        expires_days: int,
    ) -> None:
        discount_percent = bonus_value if bonus_type == "percent" else None
        discount_amount = bonus_value if bonus_type == "amount" else None
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO promo_codes(code, discount_percent, discount_amount, max_uses, expires_at, is_active)
                VALUES($1, $2, $3, $4, NOW() + ($5 || ' days')::interval, TRUE)
                ON CONFLICT (code) DO UPDATE
                  SET discount_percent=EXCLUDED.discount_percent,
                      discount_amount=EXCLUDED.discount_amount,
                      max_uses=EXCLUDED.max_uses,
                      expires_at=EXCLUDED.expires_at,
                      is_active=TRUE
                """,
                code,
                discount_percent,
                discount_amount,
                max_uses,
                expires_days,
            )
    async def all_user_ids(self) -> list[int]:
        async with self.pool.acquire() as con:
            rows = await con.fetch("SELECT id FROM users WHERE is_blocked=FALSE")
            return [int(r["id"]) for r in rows]
    async def get_app_setting(self, key: str) -> Optional[str]:
        async with self.pool.acquire() as con:
            return await con.fetchval("SELECT value FROM app_settings WHERE key=$1", key)

    async def set_app_setting(self, key: str, value: str) -> None:
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO app_settings(key, value) VALUES($1,$2) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                key,
                value,
            )

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
    async def catalog(self) -> dict[str, dict[str, Any]]:
        response = await self._request({"action": "getPrices"})
        try:
            return json.loads(response)
        except Exception as exc:
            raise RuntimeError(f"Cannot parse catalog: {response[:300]}") from exc
    async def country_titles(self) -> dict[str, str]:
        try:
            response = await self._request({"action": "getCountries"})
            data = json.loads(response)
        except Exception:
            return {}
        out: dict[str, str] = {}
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, str):
                    out[str(k)] = v
                elif isinstance(v, dict):
                    title = (
                        v.get("name")
                        or v.get("title")
                        or v.get("name_ru")
                        or v.get("country")
                        or v.get("country_name")
                    )
                    if title:
                        out[str(k)] = str(title)
        return out
    async def buy_number(
        self,
        service: str,
        country: Optional[str],
        max_price: Optional[Decimal] = None,
        provider_ids: Optional[str] = None,
        except_provider_ids: Optional[str] = None,
    ) -> tuple[str, str]:
        payload: dict[str, Any] = {"action": "getNumber", "service": service}
        payload["country"] = country if country else "any"
        if max_price is not None:
            payload["maxPrice"] = str(max_price)
        if provider_ids:
            payload["providerIds"] = provider_ids
        if except_provider_ids:
            payload["exceptProviderIds"] = except_provider_ids
        response = await self._request(payload)
        # Example ACCESS_NUMBER:123456:79990000000
        if not response.startswith("ACCESS_NUMBER"):
            known = {
                "BAD_KEY": "Неверный API-ключ Grizzly (BAD_KEY).",
                "NO_NUMBERS": "Нет доступных номеров (NO_NUMBERS).",
                "The service is prohibited for sale by administration": "Сервис запрещён к продаже администрацией.",
                "SERVICE_UNAVAILABLE_REGION": "Доступ из текущего региона ограничен (SERVICE_UNAVAILABLE_REGION).",
            }
            raise RuntimeError(known.get(response, f"Grizzly buy failed: {response}"))
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
    async def get_invoice(self, invoice_id: int) -> Optional[dict[str, Any]]:
        url = f"{self.base_url}/getInvoices"
        headers = {"Crypto-Pay-API-Token": self.token}
        params = {"invoice_ids": str(invoice_id)}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=20) as resp:
                body = await resp.text()
                parsed = json.loads(body)
                if resp.status >= 400 or not parsed.get("ok"):
                    raise RuntimeError(f"CryptoPay getInvoices failed: {body}")
                items = parsed.get("result", {}).get("items", [])
                return items[0] if items else None
class BuyState(StatesGroup):
    waiting_service = State()
    waiting_country = State()
    waiting_search = State()
class TopupState(StatesGroup):
    waiting_amount = State()
class PromoState(StatesGroup):
    waiting_code = State()
class SupportState(StatesGroup):
    waiting_username = State()
    waiting_message = State()
class AdminState(StatesGroup):
    waiting_promo = State()
    waiting_broadcast = State()
    waiting_admin_user = State()
    waiting_user_query = State()
    waiting_balance_edit = State()
    waiting_force_vip = State()
    waiting_required_subs = State()
    waiting_required_subs_add = State()
SETTINGS: Optional[Config] = None
bot: Optional[Bot] = None
dp = Dispatcher()
pool: asyncpg.Pool
db: DB
grizzly: GrizzlyClient
crypto: CryptoPayClient
active_polling_tasks: dict[int, asyncio.Task] = {}
RUNTIME_ADMIN_IDS: set[int] = set()
VERIFIED_SUB_USERS: set[int] = set()

def _parse_admin_ids(raw: str) -> set[int]:
    return {int(x.strip()) for x in (raw or "").split(",") if x.strip().isdigit()}
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
def main_kb(user_id: Optional[int] = None) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="📱 Купить номер"), KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="📞 Поддержка"), KeyboardButton(text="💰 Баланс")],
    ]
    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="🛠 Админка")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
def is_admin(user_id: int) -> bool:
    cfg = get_settings()
    if cfg.owner_chat_id and user_id == cfg.owner_chat_id:
        return True
    return user_id in RUNTIME_ADMIN_IDS
def admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💸 Товары и цены", callback_data="admin:pricing")],
            [InlineKeyboardButton(text="👤 Управление пользователями", callback_data="admin:users")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
            [InlineKeyboardButton(text="🎟 Промокоды", callback_data="admin:promo")],
            [InlineKeyboardButton(text="📣 Рассылки", callback_data="admin:broadcast")],
            [InlineKeyboardButton(text="🚨 Алерты", callback_data="admin:alerts")],
            [InlineKeyboardButton(text="✅ Обязательная подписка", callback_data="admin:required_sub")],
            [InlineKeyboardButton(text="👤 Назначить администратора", callback_data="admin:add_admin")],
            [InlineKeyboardButton(text="🗑 Удалить администратора", callback_data="admin:remove_admin")],
        ]
    )

def profile_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⭐ Избранное", callback_data="profile:fav")],
            [InlineKeyboardButton(text="🎁 Промокод", callback_data="profile:promo")],
            [InlineKeyboardButton(text="📦 История заказов", callback_data="profile:history")],
            [InlineKeyboardButton(text="👥 Реф программа", callback_data="profile:ref")],
            [InlineKeyboardButton(text="❓ FAQ", callback_data="profile:faq")],
        ]
    )

def admin_pricing_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Глобальная/индивидуальная наценка", callback_data="admin:pricing:markup")],
            [InlineKeyboardButton(text="Скрыть/показать сервис или страну", callback_data="admin:pricing:toggle")],
            [InlineKeyboardButton(text="Блэклист стран", callback_data="admin:pricing:blacklist")],
        ]
    )

COUNTRY_RU_MAP = {
    "gb": "Великобритания", "us": "США", "pt": "Португалия", "id": "Индонезия", "tr": "Турция", "th": "Таиланд",
    "ru": "Россия", "kz": "Казахстан", "ua": "Украина", "de": "Германия", "pl": "Польша", "by": "Беларусь",
}
def country_ru(code: str) -> str:
    c = str(code).lower()
    return COUNTRY_RU_MAP.get(c, str(code).upper())

COUNTRY_NUMERIC_MAP = {
    "0": "Россия",
    "1": "Украина",
    "2": "Казахстан",
    "3": "Китай",
    "4": "Филиппины",
    "5": "Мьянма",
    "6": "Индонезия",
    "10": "Киргизия",
    "11": "Камбоджа",
    "12": "США",
    "13": "Израиль",
    "14": "Гонконг",
    "15": "Польша",
    "16": "Великобритания",
}

def human_country_title(code: str, provider_titles: Optional[dict[str, str]] = None) -> str:
    key = str(code)
    title = (provider_titles or {}).get(key) or (provider_titles or {}).get(key.lower())
    if title and not str(title).isdigit():
        return str(title)
    if key in COUNTRY_NUMERIC_MAP:
        return COUNTRY_NUMERIC_MAP[key]
    ru = country_ru(key)
    return ru if not ru.isdigit() else key

def required_subs_links(raw: Optional[str]) -> list[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()][:5]

def channel_ref_for_membership(link_or_chat: str) -> Optional[str]:
    value = link_or_chat.strip()
    if value.startswith("@"):
        return value
    if value.startswith("https://t.me/") or value.startswith("http://t.me/"):
        tail = value.split("t.me/", 1)[1].split("?", 1)[0].strip("/")
        if tail and "/" not in tail and not tail.startswith("+"):
            return f"@{tail}"
    if value.startswith("-100"):
        return value
    return None

def required_subs_kb(channels: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, ch in enumerate(channels, start=1):
        if ch.startswith("http://") or ch.startswith("https://"):
            url = ch
        elif ch.startswith("@"):
            url = f"https://t.me/{ch[1:]}"
        else:
            url = ch
        rows.append([InlineKeyboardButton(text=f"Подписаться {idx}", url=url)])
    rows.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="sub:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_required_subs_manage_kb(channels: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, ch in enumerate(channels):
        rows.append([InlineKeyboardButton(text=f"❌ Удалить {ch}", callback_data=f"admin:reqsub:del:{idx}")])
    rows.append([InlineKeyboardButton(text="➕ Добавить ссылку", callback_data="admin:reqsub:add")])
    rows.append([InlineKeyboardButton(text="🗑 Очистить всё", callback_data="admin:reqsub:clear")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_country_items(catalog: dict[str, dict[str, Any]], service_code: str, provider_titles: Optional[dict[str, str]] = None) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = []
    # Format A: {country_code: {service_code: {...}}}
    if isinstance(catalog, dict) and service_code not in catalog:
        source_items = []
        for code, services in sorted(catalog.items()):
            if not isinstance(services, dict) or service_code not in services:
                continue
            source_items.append((code, services.get(service_code) or {}))
    else:
        # Format B: {service_code: {country_code: {...}}}
        service_block = catalog.get(service_code, {}) if isinstance(catalog, dict) else {}
        source_items = sorted(service_block.items()) if isinstance(service_block, dict) else []

    for code, raw in source_items:
        count = raw.get("count", raw.get("qty", 0))
        try:
            in_stock = int(str(count)) > 0
        except Exception:
            in_stock = str(count).strip() not in {"", "0", "0.0", "None", "none", "False", "false"}
        if not in_stock:
            continue
        cost = raw.get("cost", "?")
        region_title = human_country_title(str(code), provider_titles)
        title = f"{region_title} | ${cost} | шт:{count}"
        items.append((str(code), title))
    return items
SERVICE_BUTTONS_PAGES: list[list[tuple[str, str]]] = [
    [
        ("tg", "🔥 Telegram"), ("wa", "🔥 WhatsApp"), ("ig", "Instagram"), ("tt", "🔥 TikTok"),
        ("vk", "ВКонтакте"), ("ok", "Одноклассники"), ("fb", "Facebook"), ("tinder", "Tinder"),
        ("vi", "Viber"), ("bd", "Badoo"), ("discord", "Discord"),
    ],
    [
        ("go", "Gmail"), ("tw", "Twitter"), ("steam", "Steam"), ("ya", "Яндекс"),
        ("yh", "Yahoo"), ("pp", "PayPal"), ("chatgpt", "ChatGPT"), ("nf", "Netflix"),
        ("delivery", "Delivery"), ("ps", "PostScript"),
    ],
    [
        ("apple", "Apple"), ("ms", "Microsoft"), ("uber", "Uber"), ("ebay", "eBay"),
        ("bolt", "Bolt"), ("ali", "AliExpress"), ("alibaba", "Alibaba"), ("mamba", "Mamba"),
        ("be", "Bee"),
    ],
]
def service_page_kb(page: int) -> InlineKeyboardMarkup:
    page = max(0, min(page, len(SERVICE_BUTTONS_PAGES)-1))
    rows = [[InlineKeyboardButton(text=title, callback_data=f"svc:{code}")] for code, title in SERVICE_BUTTONS_PAGES[page]]
    nav = [InlineKeyboardButton(text="🔎 Поиск сервиса", callback_data="svc:search")]
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"svcpage:{page-1}"))
    if page < len(SERVICE_BUTTONS_PAGES)-1:
        nav.append(InlineKeyboardButton(text="➡️ Далее", callback_data=f"svcpage:{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="➕ Пополнить баланс", callback_data="buy:topup")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
def _paged_buttons(
    items: list[tuple[str, str]],
    item_prefix: str,
    page: int,
    page_prefix: Optional[str] = None,
    page_size: int = 12,
) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(items) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    chunk = items[page * page_size : (page + 1) * page_size]
    rows = [[InlineKeyboardButton(text=title, callback_data=f"{item_prefix}:{code}")] for code, title in chunk]
    page_prefix = page_prefix or f"{item_prefix}pg"
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{page_prefix}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{page_prefix}:{page+1}"))
    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)
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

async def has_required_subscriptions(user_id: int) -> tuple[bool, list[str]]:
    if user_id in VERIFIED_SUB_USERS:
        return True, []
    if await db.is_subscription_verified(user_id):
        VERIFIED_SUB_USERS.add(user_id)
        return True, []
    raw = await db.get_app_setting("required_subscriptions")
    if not raw:
        return True, []
    channels = required_subs_links(raw)
    if not channels:
        return True, []
    missing: list[str] = []
    for chat in channels:
        chat_ref = channel_ref_for_membership(chat)
        if not chat_ref:
            # Непроверяемые ссылки (invite/private) не блокируют вход.
            continue
        try:
            member = await get_bot().get_chat_member(chat_ref, user_id)
            if member.status in {"left", "kicked"}:
                missing.append(chat)
        except Exception as exc:
            # Если бот не может проверить подписку технически (нет прав/тип ссылки),
            # не зацикливаем пользователя на "подпишитесь".
            logger.warning("Subscription check skipped for %s: %s", chat_ref, exc)
            continue
    return len(missing) == 0, missing

def vip_info(user_row: asyncpg.Record) -> tuple[bool, Decimal, Decimal]:
    cfg = get_settings()
    spent = Decimal(str(user_row["total_spent"]))
    vip_until = user_row["vip_until"]
    forced = bool(vip_until) and vip_until > datetime.now(timezone.utc)
    auto = spent >= cfg.vip_threshold_total_spent
    remain = max(Decimal("0"), cfg.vip_threshold_total_spent - spent)
    return forced or auto, spent, remain
@dp.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_user(message)
    ok, missing = await has_required_subscriptions(message.from_user.id)
    if not ok:
        text = "\n".join(f"• {x}" for x in missing)
        await message.answer(
            "Перед стартом подпишитесь на каналы/группы:\n" + text,
            reply_markup=required_subs_kb(missing),
        )
        return
    VERIFIED_SUB_USERS.add(message.from_user.id)
    await db.mark_subscription_verified(message.from_user.id)
    me = await db.user(message.from_user.id)
    await message.answer_photo(
        photo="https://imgur.gg/f/ArQ0tkg",
        caption=(
            "Привет, пользователь! 👋\n"
            "Добро пожаловать в наш SMS-бот. Выбирай, что хочешь.\n\n"
            "<b>Что умеет бот:</b>\n"
            "• Покупка виртуальных номеров по странам и сервисам\n"
            "• Выбор количества и подтверждение покупки\n"
            "• Автополлинг SMS-кодов\n"
            "• Пополнение баланса и история заказов\n"
            "• Поддержка и профиль\n\n"
            f"Текущий баланс: <b>{Decimal(str(me['balance'])):.2f}</b> USDT"
        ),
        reply_markup=main_kb(message.from_user.id),
    )
@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()
@dp.message(F.text == "💰 Баланс")
async def show_balance(message: Message):
    await ensure_user(message)
    me = await db.user(message.from_user.id)
    balance = Decimal(str(me["balance"]))
    await message.answer(
        "<b>Ваш баланс</b>\n"
        f"Текущий остаток: <b>{balance:.2f} USDT</b>\n"
        f"Купить можно примерно: {int(balance // Decimal('0.15'))} номеров (при цене 0.15)",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="➕ Пополнить баланс", callback_data="topup:start")]]
        ),
    )

@dp.message(F.text == "👤 Профиль")
async def show_profile(message: Message):
    await ensure_user(message)
    me = await db.user(message.from_user.id)
    is_vip, spent, remain = vip_info(me)
    await message.answer_photo(
        photo="https://imgur.gg/f/2LZUA0w",
        caption=(
            "<b>Профиль</b>\n"
            f"ID: <code>{message.from_user.id}</code>\n"
            f"Баланс: <b>{Decimal(str(me['balance'])):.2f} USDT</b>\n"
            f"VIP: <b>{'Да' if is_vip else 'Нет'}</b>\n"
            f"До VIP: <b>{remain:.2f} USDT</b>"
        ),
        reply_markup=profile_inline_kb(),
    )

@dp.callback_query(F.data == "profile:fav")
async def profile_fav_cb(cb: CallbackQuery):
    await cb.answer()
    rows = await db.recent_orders(cb.from_user.id, limit=20)
    seen = set()
    items = []
    for r in rows:
        key = (r["service_code"], r["country_code"])
        if key in seen:
            continue
        seen.add(key)
        items.append(f"• {r['service_code'].upper()} / {country_ru(r['country_code'])}")
    await cb.message.answer("<b>Избранное</b>\n" + ("\n".join(items) if items else "Пока пусто"))

@dp.callback_query(F.data == "profile:promo")
async def profile_promo_cb(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.set_state(PromoState.waiting_code)
    await cb.message.answer("Введите промокод")

@dp.callback_query(F.data == "profile:history")
async def profile_history_cb(cb: CallbackQuery):
    await cb.answer()
    rows = await db.recent_orders(cb.from_user.id)
    if not rows:
        await cb.message.answer("История пока пуста.")
        return
    lines = ["<b>История заказов:</b>"]
    for r in rows:
        lines.append(f"#{r['id']} | {r['service_code'].upper()}/{country_ru(r['country_code'])} | {r['status']} | {Decimal(str(r['client_price'])):.2f}")
    await cb.message.answer("\n".join(lines))

@dp.callback_query(F.data == "profile:ref")
async def profile_ref_cb(cb: CallbackQuery):
    await cb.answer()
    link = f"https://t.me/{(await get_bot().get_me()).username}?start=ref_{cb.from_user.id}"
    await cb.message.answer(
        "<b>Реферальная программа</b>\n"
        f"Ваш процент: {get_settings().referral_percent}%\n"
        f"Ссылка: {link}"
    )

@dp.callback_query(F.data == "profile:faq")
async def profile_faq_cb(cb: CallbackQuery):
    await cb.answer()
    await cb.message.answer_photo(
        photo="https://imgur.gg/f/78p7cPE",
        caption=(
            "<b>FAQ</b>\n"
            "• Почему не пришло SMS? — Иногда сервис не отправляет код, в таком случае после таймаута деньги возвращаются.\n"
            "• Как пополнить баланс? — Через кнопку '➕ Пополнить баланс'.\n"
            "• Что такое VIP? — При достижении оборота включается скидка на все номера."
        ),
    )
@dp.message(F.text == "📱 Купить номер")
async def buy_start(message: Message, state: FSMContext):
    await ensure_user(message)
    await state.set_state(BuyState.waiting_service)
    await message.answer_photo(
        photo="https://imgur.gg/f/qFiCKpA",
        caption="Список сервисов:",
        reply_markup=service_page_kb(0),
    )

@dp.callback_query(F.data == "buy:topup")
async def buy_topup_cb(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await topup_start_cb(cb, state)
@dp.callback_query(F.data.startswith("svcpage:"))
async def buy_service_page(cb: CallbackQuery):
    page = int(cb.data.split(":", 1)[1])
    await cb.message.edit_reply_markup(reply_markup=service_page_kb(page))
    await cb.answer()
@dp.callback_query(F.data == "svc:search")
async def buy_service_search_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(BuyState.waiting_search)
    await cb.message.answer("Введите название сервиса или код (например telegram / tg):")
    await cb.answer()
@dp.message(BuyState.waiting_search)
async def buy_service_search_text(message: Message, state: FSMContext):
    q = message.text.strip().lower()
    for page in SERVICE_BUTTONS_PAGES:
        for code, title in page:
            if q in code.lower() or q in title.lower():
                await state.set_state(BuyState.waiting_country)
                data = await grizzly.catalog()
                titles = await grizzly.country_titles()
                items = build_country_items(data, code, titles)
                if not items:
                    await message.answer(f"Для сервиса {title} сейчас нет доступных номеров.")
                    await state.set_state(BuyState.waiting_service)
                    await message.answer("Выберите сервис:", reply_markup=service_page_kb(0))
                    return
                kb = _paged_buttons(items, f"cnt:{code}", page=0, page_prefix=f"cntpg:{code}")
                await state.update_data(service_code=code, country_items=items)
                await message.answer(f"Выберите страну для {title}:", reply_markup=kb)
                return
    await message.answer("Сервис не найден. Попробуйте ещё раз или вернитесь кнопкой 'Купить номер'.")
@dp.callback_query(F.data.startswith("svc:"))
async def buy_pick_service(cb: CallbackQuery, state: FSMContext):
    if cb.data == "svc:search":
        return
    service_code = cb.data.split(":", 1)[1]
    try:
        catalog = await grizzly.catalog()
        titles = await grizzly.country_titles()
    except Exception as exc:
        await cb.answer(f"Ошибка загрузки каталога: {exc}", show_alert=True)
        return
    items = build_country_items(catalog, service_code, titles)
    if not items:
        await cb.answer("Номера для сервиса сейчас недоступны", show_alert=True)
        return
    kb = _paged_buttons(items, f"cnt:{service_code}", page=0, page_prefix=f"cntpg:{service_code}")
    await state.set_state(BuyState.waiting_country)
    await state.update_data(service_code=service_code, country_items=items)
    if cb.message.photo:
        await cb.message.edit_caption(caption="Выберите страну:", reply_markup=kb)
    else:
        await cb.message.edit_text("Выберите страну:", reply_markup=kb)
    await cb.answer()
@dp.callback_query(F.data.startswith("cntpg:"))
async def buy_country_page(cb: CallbackQuery, state: FSMContext):
    _, service_code, page_str = cb.data.split(":", 2)
    page = int(page_str)
    data = await state.get_data()
    items = data.get("country_items", [])
    await cb.message.edit_reply_markup(
        reply_markup=_paged_buttons(items, f"cnt:{service_code}", page=page, page_prefix=f"cntpg:{service_code}")
    )
    await cb.answer()
@dp.callback_query(F.data.startswith("cnt:"))
async def buy_pick_country(cb: CallbackQuery, state: FSMContext):
    _, service_code, country_code = cb.data.split(":", 2)
    await state.set_state(BuyState.waiting_country)
    qty_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data=f"qty:{service_code}:{country_code}:1"), InlineKeyboardButton(text="2", callback_data=f"qty:{service_code}:{country_code}:2"), InlineKeyboardButton(text="3", callback_data=f"qty:{service_code}:{country_code}:3")],
            [InlineKeyboardButton(text="4", callback_data=f"qty:{service_code}:{country_code}:4"), InlineKeyboardButton(text="5", callback_data=f"qty:{service_code}:{country_code}:5")],
        ]
    )
    text = f"Вы выбрали страну: <b>{country_ru(country_code)}</b>\nВыберите количество номеров:"
    if cb.message.photo:
        await cb.message.edit_caption(caption=text, reply_markup=qty_kb)
    else:
        await cb.message.edit_text(text, reply_markup=qty_kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("qty:"))
async def buy_pick_quantity(cb: CallbackQuery):
    _, service_code, country_code, qty_raw = cb.data.split(":", 3)
    qty = max(1, min(5, int(qty_raw)))
    try:
        provider_price = await grizzly.get_price(service_code, country_code)
        final_price = await db.calculate_price(service_code, provider_price, cb.from_user.id)
    except Exception:
        await cb.answer("Не удалось рассчитать цену", show_alert=True)
        return
    total = final_price * qty
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить покупку", callback_data=f"buyconfirm:{service_code}:{country_code}:{qty}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="buyconfirm:cancel")],
        ]
    )
    text = (
        f"<b>Подтверждение покупки</b>\n"
        f"Сервис: <b>{service_code.upper()}</b>\n"
        f"Страна: <b>{country_ru(country_code)}</b>\n"
        f"Количество: <b>{qty}</b>\n"
        f"Цена за 1: <b>{final_price:.2f} USDT</b>\n"
        f"Итого: <b>{total:.2f} USDT</b>"
    )
    if cb.message.photo:
        await cb.message.edit_caption(caption=text, reply_markup=kb)
    else:
        await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()


@dp.callback_query(F.data == "buyconfirm:cancel")
async def buy_confirm_cancel(cb: CallbackQuery):
    await cb.answer("Покупка отменена")
    await cb.message.answer("Покупка отменена. Выберите сервис снова.", reply_markup=service_page_kb(0))


@dp.callback_query(F.data.startswith("buyconfirm:"))
async def buy_confirm_apply(cb: CallbackQuery, state: FSMContext):
    _, service_code, country_code, qty_raw = cb.data.split(":", 3)
    qty = max(1, min(5, int(qty_raw)))
    user_id = cb.from_user.id
    success_count = 0
    for _ in range(qty):
        try:
            provider_price = await grizzly.get_price(service_code, country_code)
        except Exception:
            break
        final_price = await db.calculate_price(service_code, provider_price, user_id)
        order_id = await db.reserve_order(user_id, service_code, country_code, final_price, promo_code=None)
        if not order_id:
            if success_count == 0:
                text = f"Недостаточно средств. Цена: <b>{final_price:.2f} USDT</b>\nПополните баланс."
                kb_low = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Пополнить", callback_data="topup:start")]])
                await cb.message.answer(text, reply_markup=kb_low)
            break
        try:
            activation_id, number = await grizzly.buy_number(service_code, country_code)
            await db.set_order_provider(order_id, activation_id, number, provider_price)
        except Exception as exc:
            await db.cancel_order(order_id, reason="provider_error")
            err = str(exc)
            if "NO_BALANCE" in err.upper() or "NO_MONEY" in err.upper() or "balance" in err.lower():
                err = "у провайдера Grizzly нулевой баланс (пополните Grizzly API)"
            await cb.message.answer(f"Ошибка при выдаче номера: {err}. Средства возвращены.")
            break
        task = asyncio.create_task(poll_sms(order_id, user_id))
        active_polling_tasks[order_id] = task
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Получить код ещё раз", callback_data=f"sms:again:{order_id}")],
                [InlineKeyboardButton(text="❌ Отменить номер", callback_data=f"sms:cancel:{order_id}")],
                [InlineKeyboardButton(text="🔂 Повторить заказ", callback_data=f"sms:repeat:{service_code}:{country_code}")],
            ]
        )
        success_text = "Номер успешно выдан.\n" f"<b>Номер:</b> <code>{number}</code>\n" "Ожидаю SMS-код (до 20 минут)..."
        await cb.message.answer(success_text, reply_markup=kb)
        success_count += 1
    await state.clear()
    await cb.answer("Готово")

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
    qty_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data=f"qty:{service_code}:{country_code}:1"), InlineKeyboardButton(text="2", callback_data=f"qty:{service_code}:{country_code}:2"), InlineKeyboardButton(text="3", callback_data=f"qty:{service_code}:{country_code}:3")],
            [InlineKeyboardButton(text="4", callback_data=f"qty:{service_code}:{country_code}:4"), InlineKeyboardButton(text="5", callback_data=f"qty:{service_code}:{country_code}:5")],
        ]
    )
    await cb.message.answer(f"Повтор заказа {service_code.upper()} / {country_ru(country_code)}. Выберите количество:", reply_markup=qty_kb)
    await cb.answer()
@dp.message(F.text == "📦 История заказов")
async def history(message: Message):
    rows = await db.recent_orders(message.from_user.id)
    if not rows:
        await message.answer("История пока пуста.")
        return
    lines = ["<b>История заказов:</b>"]
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
@dp.message(TopupState.waiting_amount, F.text.in_({"📱 Купить номер", "👤 Профиль", "💰 Баланс", "➕ Пополнить баланс", "📦 История заказов", "🎁 Промокод", "👥 Реферальная программа", "❓ FAQ", "📞 Поддержка", "🛠 Админка"}))
async def topup_amount_menu_break(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Действие пополнения отменено.")

@dp.message(TopupState.waiting_amount)
async def topup_amount(message: Message, state: FSMContext):
    try:
        amount = Decimal(message.text.replace(",", "."))
        if amount < get_settings().min_topup_amount:
            await message.answer(f"Минимальная сумма: {get_settings().min_topup_amount} USDT")
            return
    except Exception:
        await message.answer("Введите число (например, 5.5)")
        return
    topup_id = await db.create_topup(message.from_user.id, amount, "USDT")
    try:
        invoice = await crypto.create_invoice(amount, "USDT", f"topup_{topup_id}", "Пополнение баланса SMS")
        await db.bind_invoice(topup_id, invoice["invoice_id"])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=invoice["pay_url"])],
            [InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check_pay:{invoice['invoice_id']}")]
        ])
        await message.answer(f"Счет на {amount} USDT создан!", reply_markup=kb)
    except Exception as e:
        logger.error(f"Invoice error: {e}")
        await message.answer("Ошибка платежной системы. Попробуйте позже.")
    await state.clear()
@dp.callback_query(F.data.startswith("check_pay:"))
async def check_payment(cb: CallbackQuery):
    invoice_id = int(cb.data.split(":")[1])
    try:
        invoice = await crypto.get_invoice(invoice_id)
    except Exception as exc:
        await cb.answer(f"Не удалось проверить счет: {exc}", show_alert=True)
        return
    if not invoice or invoice.get("status") != "paid":
        await cb.answer("Счёт ещё не оплачен в CryptoPay", show_alert=True)
        return
    res = await db.credit_paid_topup(invoice_id)
    if res:
        user_id, amount, _ = res
        await cb.message.edit_text(f"✅ Оплата принята! Баланс пополнен на {amount} USDT")
        await cb.answer()
    else:
        await cb.answer("Оплата пока не найдена или уже зачислена", show_alert=True)
@dp.message(F.text == "👥 Реферальная программа")
async def referral(message: Message):
    link = f"https://t.me/{(await get_bot().get_me()).username}?start=ref_{message.from_user.id}"
    await message.answer(
        "<b>Реферальная программа</b>\n"
        f"Ваш процент: {get_settings().referral_percent}% от каждого пополнения реферала.\n"
        f"Ссылка: {link}"
    )
@dp.message(Command("admin"))
@dp.message(F.text == "🛠 Админка")
async def admin_entry(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Доступ запрещён")
        return
    await message.answer(
        "<b>Панель администратора</b>\n\n"
        "Товары и цены • Пользователи • Статистика • Промокоды • Рассылки • Алерты • Обязательная подписка",
        reply_markup=admin_kb(),
    )

@dp.callback_query(F.data == "admin:pricing")
async def admin_pricing(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.answer("Раздел цен:", reply_markup=admin_pricing_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin:pricing:markup")
async def admin_pricing_markup(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.answer("• Глобальная наценка на все сервисы или индивидуально по каждому.\nЭто чтобы изменять цены на все или некоторые сервисы.")
    await cb.answer()

@dp.callback_query(F.data == "admin:pricing:toggle")
async def admin_pricing_toggle(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.answer("• Скрыть / показать отдельный сервис или страну.\nЭто чтобы показывать или скрывать страну или сервис.")
    await cb.answer()

@dp.callback_query(F.data == "admin:pricing:blacklist")
async def admin_pricing_blacklist(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.answer("• Блэклист стран — убрать страны с низким качеством номеров.\nЭто чтобы убирать некачественные страны.")
    await cb.answer()

@dp.callback_query(F.data == "admin:users")
async def admin_users(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminState.waiting_user_query)
    await render_admin_users_page(cb.message, page=0)
    await cb.message.answer("Или введите ID / @username пользователя для поиска:")
    await cb.answer()


async def render_admin_users_page(message: Message, page: int):
    page_size = 15
    async with db.pool.acquire() as con:
        total = int(await con.fetchval("SELECT COUNT(*) FROM users"))
        offset = max(0, page) * page_size
        rows = await con.fetch(
            "SELECT id, username FROM users ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            page_size,
            offset,
        )
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    kb_rows = []
    for r in rows:
        uname = f"@{r['username']}" if r['username'] else f"ID {r['id']}"
        kb_rows.append([InlineKeyboardButton(text=uname, callback_data=f"admin:users:pick:{r['id']}:{page}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:users:pg:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:users:pg:{page+1}"))
    kb_rows.append(nav)
    await message.answer("Выберите пользователя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))


@dp.callback_query(F.data.startswith("admin:users:pg:"))
async def admin_users_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    page = int(cb.data.rsplit(":", 1)[1])
    await render_admin_users_page(cb.message, page)
    await cb.answer()


@dp.callback_query(F.data.startswith("admin:users:pick:"))
async def admin_users_pick(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    _, _, _, uid_raw, page_raw = cb.data.split(":", 4)
    uid = int(uid_raw)
    page = int(page_raw)
    async with db.pool.acquire() as con:
        rec = await con.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    if not rec:
        await cb.answer("Пользователь не найден", show_alert=True)
        return
    orders = await db.recent_orders(int(rec["id"]), limit=10)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Баланс", callback_data=f"admin:user:baladd:{rec['id']}")],
        [InlineKeyboardButton(text="➖ Баланс", callback_data=f"admin:user:balsub:{rec['id']}")],
        [InlineKeyboardButton(text="VIP ON", callback_data=f"admin:user:vipon:{rec['id']}")],
        [InlineKeyboardButton(text="VIP OFF", callback_data=f"admin:user:vipoff:{rec['id']}")],
        [InlineKeyboardButton(text="Бан", callback_data=f"admin:user:block:{rec['id']}")],
        [InlineKeyboardButton(text="Разбан", callback_data=f"admin:user:unblock:{rec['id']}")],
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=f"admin:users:back:{page}")],
    ])
    await cb.message.answer(
        f"ID: <code>{rec['id']}</code>\n"
        f"Username: @{rec['username'] or '-'}\n"
        f"Баланс: {Decimal(str(rec['balance'])):.2f}\n"
        f"История заказов (10): {len(orders)}",
        reply_markup=kb,
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("admin:users:back:"))
async def admin_users_back(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    page = int(cb.data.rsplit(":", 1)[1])
    await render_admin_users_page(cb.message, page)
    await cb.answer()
@dp.callback_query(F.data == "admin:stats")
async def admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    stats = await db.admin_overall_stats()
    async with db.pool.acquire() as con:
        day = await con.fetchrow("SELECT COUNT(*) c, COALESCE(SUM(client_price),0) s FROM orders WHERE status='completed' AND completed_at >= NOW()-INTERVAL '1 day'")
        week = await con.fetchrow("SELECT COUNT(*) c, COALESCE(SUM(client_price),0) s FROM orders WHERE status='completed' AND completed_at >= NOW()-INTERVAL '7 days'")
        month = await con.fetchrow("SELECT COUNT(*) c, COALESCE(SUM(client_price),0) s FROM orders WHERE status='completed' AND completed_at >= NOW()-INTERVAL '30 days'")
        uniq = await con.fetchval("SELECT COUNT(DISTINCT user_id) FROM orders WHERE status='completed' AND completed_at >= NOW()-INTERVAL '30 days'")
        top = await con.fetch("SELECT service_code, COUNT(*) cnt FROM orders WHERE status='completed' GROUP BY service_code ORDER BY cnt DESC LIMIT 5")
    top_txt = "\n".join(f"• {r['service_code']}: {r['cnt']}" for r in top) or "нет данных"
    await cb.message.answer(
        "<b>Статистика</b>\n"
        f"День: {day['c']} заказов / {Decimal(str(day['s'])):.2f} USDT\n"
        f"Неделя: {week['c']} / {Decimal(str(week['s'])):.2f} USDT\n"
        f"Месяц: {month['c']} / {Decimal(str(month['s'])):.2f} USDT\n"
        f"Уникальные покупатели (30д): {uniq}\n"
        f"Топ сервисов:\n{top_txt}\n\n"
        f"Пользователей всего: {stats['users_total']}\n"
        f"Заказов создано: {stats['orders_total']}\n"
        f"Заказов завершено: {stats['completed_total']}\n"
        f"Заказов в выполнении: {stats['in_progress_total']}\n"
        f"Общий баланс пользователей: {Decimal(str(stats['users_balance_total'])):.2f} USDT\n"
        f"Сумма пополнений: {Decimal(str(stats['topup_sum_total'])):.2f} USDT"
    )
    await cb.answer()

@dp.message(AdminState.waiting_user_query)
async def admin_user_lookup(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    query = message.text.strip()
    async with db.pool.acquire() as con:
        if query.startswith("@"):
            rec = await con.fetchrow("SELECT * FROM users WHERE username=$1", query[1:])
        elif query.isdigit():
            rec = await con.fetchrow("SELECT * FROM users WHERE id=$1", int(query))
        else:
            rec = None
    if not rec:
        await message.answer("Пользователь не найден")
        return
    orders = await db.recent_orders(int(rec["id"]), limit=10)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Баланс", callback_data=f"admin:user:baladd:{rec['id']}")],
        [InlineKeyboardButton(text="➖ Баланс", callback_data=f"admin:user:balsub:{rec['id']}")],
        [InlineKeyboardButton(text="VIP ON", callback_data=f"admin:user:vipon:{rec['id']}")],
        [InlineKeyboardButton(text="VIP OFF", callback_data=f"admin:user:vipoff:{rec['id']}")],
        [InlineKeyboardButton(text="Блок", callback_data=f"admin:user:block:{rec['id']}")],
        [InlineKeyboardButton(text="Разблок", callback_data=f"admin:user:unblock:{rec['id']}")],
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="admin:users:back:0")],
    ])
    await message.answer(
        f"ID: <code>{rec['id']}</code>\n"
        f"Username: @{rec['username'] or '-'}\n"
        f"Баланс: {Decimal(str(rec['balance'])):.2f}\n"
        f"История заказов (10): {len(orders)}\n"
        f"Реф-статистика: MVP",
        reply_markup=kb,
    )

@dp.callback_query(F.data == "admin:promo")
async def admin_promo_panel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.answer("Промокоды: создание, редактирование, удаление и статистика использования (MVP-панель)")
    await cb.answer()

@dp.callback_query(F.data == "admin:alerts")
async def admin_alerts_panel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.message.answer("Алерты: низкий баланс Grizzly, крупные пополнения, ошибки Grizzly API.")
    await cb.answer()

@dp.callback_query(F.data == "admin:required_sub")
async def admin_required_subs_panel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    current = required_subs_links(await db.get_app_setting("required_subscriptions"))
    await cb.message.answer(
        "Управление обязательной подпиской:\n"
        + ("\n".join(f"• {x}" for x in current) if current else "Список пуст"),
        reply_markup=admin_required_subs_manage_kb(current),
    )
    await cb.answer()

@dp.callback_query(F.data == "admin:reqsub:add")
async def admin_required_subs_add_start(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminState.waiting_required_subs_add)
    await cb.message.answer("Пришлите ссылку канала/группы (https://... или @username)")
    await cb.answer()

@dp.message(AdminState.waiting_required_subs_add)
async def admin_required_subs_add_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    new_link = message.text.strip()
    channels = required_subs_links(await db.get_app_setting("required_subscriptions"))
    channels.append(new_link)
    channels = channels[:5]
    if len(channels) > 5:
        await message.answer("Максимум 5 каналов/групп")
        return
    await db.set_app_setting("required_subscriptions", ",".join(channels))
    await message.answer("Ссылка добавлена", reply_markup=admin_required_subs_manage_kb(channels))
    await state.clear()

@dp.callback_query(F.data.startswith("admin:reqsub:del:"))
async def admin_required_subs_del(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    idx = int(cb.data.rsplit(":", 1)[1])
    channels = required_subs_links(await db.get_app_setting("required_subscriptions"))
    if 0 <= idx < len(channels):
        channels.pop(idx)
    await db.set_app_setting("required_subscriptions", ",".join(channels))
    await cb.message.answer("Обновлено", reply_markup=admin_required_subs_manage_kb(channels))
    await cb.answer()

@dp.callback_query(F.data == "admin:reqsub:clear")
async def admin_required_subs_clear(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await db.set_app_setting("required_subscriptions", "")
    await cb.message.answer("Обязательная подписка очищена", reply_markup=admin_required_subs_manage_kb([]))
    await cb.answer()

@dp.message(AdminState.waiting_required_subs)
async def admin_required_subs_save_legacy(message: Message, state: FSMContext):
    # legacy fallback
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    channels = [x.strip() for x in message.text.split(",") if x.strip()][:5]
    await db.set_app_setting("required_subscriptions", ",".join(channels))
    await message.answer("Сохранено")
    await state.clear()

@dp.callback_query(F.data.startswith("admin:user:"))
async def admin_user_action(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    _, _, action, uid = cb.data.split(":", 3)
    user_id = int(uid)
    if action in {"baladd", "balsub"}:
        await state.set_state(AdminState.waiting_balance_edit)
        await state.update_data(target_user_id=user_id, balance_action=action)
        prompt = "Введите баланс для пользователя:" if action == "baladd" else "Введите сумму списания для пользователя:"
        await cb.message.answer(prompt)
        await cb.answer()
        return
    async with db.pool.acquire() as con:
        if action == "vipon":
            await con.execute("UPDATE users SET vip_until=NOW() + INTERVAL '3650 days' WHERE id=$1", user_id)
            await cb.message.answer("VIP выдан")
        elif action == "vipoff":
            await con.execute("UPDATE users SET vip_until=NULL WHERE id=$1", user_id)
            await cb.message.answer("VIP снят")
        elif action == "block":
            await con.execute("UPDATE users SET is_blocked=TRUE WHERE id=$1", user_id)
            await cb.message.answer("Пользователь заблокирован")
        elif action == "unblock":
            await con.execute("UPDATE users SET is_blocked=FALSE WHERE id=$1", user_id)
            await cb.message.answer("Пользователь разблокирован")
        else:
            await cb.message.answer("Неизвестное действие")
    await cb.answer()


@dp.message(AdminState.waiting_balance_edit)
async def admin_balance_edit_apply(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    user_id = int(data.get("target_user_id", 0))
    action = str(data.get("balance_action", ""))
    try:
        amount = Decimal(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except Exception:
        await message.answer("Введите корректную сумму, например: 10 или 15.5")
        return
    delta = amount if action == "baladd" else -amount
    await db.add_balance(
        user_id,
        delta,
        "admin_balance_edit",
        f"admin:{message.from_user.id}",
        {"action": action},
    )
    user = await db.user(user_id)
    await message.answer(
        f"Готово. Пользователь <code>{user_id}</code>, новый баланс: <b>{Decimal(str(user['balance'])):.2f}</b> USDT"
    )
    await state.clear()
@dp.callback_query(F.data == "admin:add_admin")
async def admin_add_admin_start(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminState.waiting_admin_user)
    await cb.message.answer("Введите Telegram user ID нового администратора:")
    await cb.answer()

@dp.message(AdminState.waiting_admin_user)
async def admin_add_admin_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        new_id = int(message.text.strip())
    except Exception:
        await message.answer("Неверный ID. Введите число.")
        return
    RUNTIME_ADMIN_IDS.add(new_id)
    await db.set_app_setting("admin_user_ids", ",".join(str(x) for x in sorted(RUNTIME_ADMIN_IDS)))
    await message.answer(f"Администратор {new_id} добавлен")
    await state.clear()

@dp.callback_query(F.data == "admin:remove_admin")
async def admin_remove_admin_menu(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    ids = sorted(RUNTIME_ADMIN_IDS)
    async with db.pool.acquire() as con:
        users = await con.fetch("SELECT id, username FROM users WHERE id = ANY($1::bigint[])", ids or [0])
    uname_by_id = {int(r['id']): (f"@{r['username']}" if r['username'] else None) for r in users}
    rows = []
    for x in ids:
        label = uname_by_id.get(int(x)) or f"ID {x}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"admin:remove_admin:{x}")])
    if not rows:
        await cb.message.answer("Список админов пуст")
        await cb.answer()
        return
    await cb.message.answer("Выберите администратора для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cb.answer()

@dp.callback_query(F.data.startswith("admin:remove_admin:"))
async def admin_remove_admin_apply(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    admin_id = int(cb.data.rsplit(":", 1)[1])
    if get_settings().owner_chat_id and admin_id == get_settings().owner_chat_id:
        await cb.answer("Нельзя удалить владельца", show_alert=True)
        return
    if admin_id not in RUNTIME_ADMIN_IDS:
        await cb.answer("Уже удалён", show_alert=True)
        return
    RUNTIME_ADMIN_IDS.discard(admin_id)
    await db.set_app_setting("admin_user_ids", ",".join(str(x) for x in sorted(RUNTIME_ADMIN_IDS)))
    await cb.message.answer(f"Администратор {admin_id} удалён")
    await cb.answer("Готово")

@dp.callback_query(F.data == "admin:create_promo")
async def admin_create_promo_start(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminState.waiting_promo)
    await cb.message.answer("Формат: ПРОМОКОД:ТИП:ЗНАЧЕНИЕ:ЛИМИТ:ДНЕЙ\nТип: percent (проценты) или amount (сумма)\nПример: SALE10:percent:10:100:30")
    await cb.answer()
@dp.message(AdminState.waiting_promo)
async def admin_create_promo(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    try:
        code, btype, value, uses, days = [x.strip() for x in message.text.split(":", 4)]
        btype = btype.lower()
        if btype not in {"percent", "amount"}:
            raise ValueError("bad type")
        await db.create_promo_code(code.upper(), btype, Decimal(value), int(uses), int(days))
    except Exception:
        await message.answer("Неверный формат. Пример: SALE10:percent:10:100:30")
        return
    await message.answer("Промокод создан/обновлён")
    await state.clear()
@dp.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminState.waiting_broadcast)
    await cb.message.answer("Отправьте сообщение для рассылки (текст или фото+текст).")
    await cb.answer()
@dp.message(AdminState.waiting_broadcast)
async def admin_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    user_ids = await db.all_user_ids()
    ok = 0
    for uid in user_ids:
        try:
            await get_bot().copy_message(chat_id=uid, from_chat_id=message.chat.id, message_id=message.message_id)
            ok += 1
        except Exception:
            pass
    await message.answer(f"Рассылка завершена: {ok}/{len(user_ids)}")
    await state.clear()
@dp.message(F.text == "❓ FAQ")
async def faq(message: Message):
    await message.answer_photo(
        photo="https://imgur.gg/f/78p7cPE",
        caption=(
            "<b>FAQ</b>\n"
            "• Почему не пришло SMS? — Иногда сервис не отправляет код, в таком случае после таймаута деньги возвращаются.\n"
            "• Как пополнить баланс? — Через кнопку '➕ Пополнить баланс'.\n"
            "• Что такое VIP? — При достижении оборота включается скидка на все номера."
        ),
    )
@dp.message(F.text == "📞 Поддержка")
async def support(message: Message):
    support_ref = get_settings().support_username.strip()
    support_url = support_ref
    if support_ref.startswith("@"):
        support_url = f"https://t.me/{support_ref[1:]}"
    await message.answer(
        "<b>Поддержка</b>\n"
        f"Связь: {support_ref}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="✉️ Написать в поддержку", url=support_url)]]
        ),
    )
@dp.message(F.text.startswith("SUPPORT:"))
async def support_passthrough(message: Message):
    text = message.text.replace("SUPPORT:", "", 1).strip()
    if get_settings().owner_chat_id:
        await get_bot().send_message(
            get_settings().owner_chat_id,
            f"🆘 Обращение в поддержку\nFrom: @{message.from_user.username or 'unknown'} ({message.from_user.id})\nSupport: {get_settings().support_username}\n\n{text}",
        )
        await message.answer("Сообщение отправлено в поддержку.")
@dp.message(F.text == "⭐ Избранное")
async def favorites(message: Message):
    rows = await db.recent_orders(message.from_user.id, limit=20)
    seen = set()
    items = []
    for r in rows:
        key = (r["service_code"], r["country_code"])
        if key in seen:
            continue
        seen.add(key)
        items.append(f"• {r['service_code'].upper()} / {country_ru(r['country_code'])}")
    if not items:
        await message.answer("Избранное пока пусто. Сделайте хотя бы один заказ.")
        return
    await message.answer("<b>Избранное</b>\n" + "\n".join(items))
@dp.message(F.text == "🎁 Промокод")
async def promo(message: Message, state: FSMContext):
    await state.set_state(PromoState.waiting_code)
    await message.answer("Введите промокод (например: SALE10). Для отмены нажмите любую кнопку меню.")
@dp.message(PromoState.waiting_code, F.text.in_({"📱 Купить номер", "👤 Профиль", "💰 Баланс", "➕ Пополнить баланс", "📦 История заказов", "🎁 Промокод", "👥 Реферальная программа", "❓ FAQ", "📞 Поддержка", "🛠 Админка"}))
async def promo_menu_break(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Ввод промокода отменен.")

@dp.message(PromoState.waiting_code)
async def apply_promo_code(message: Message, state: FSMContext):
    code = message.text.strip().upper().replace("PROMO:", "").strip()
    if not code or len(code) < 3 or " " in code:
        await message.answer("Неверный промокод. Пример: SALE10")
        return
    await state.clear()
    await message.answer(f"Промокод {code} сохранен для следующего заказа (MVP).")
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
# --- ФИНАЛЬНЫЙ БЛОК ЗАПУСКА ---
async def main():
    global pool, db, grizzly, crypto, bot
    config = get_settings()
    bot = Bot(
        token=config.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    try:
        pool = await asyncpg.create_pool(dsn=config.postgres_dsn)
    except asyncpg.InvalidCatalogNameError:
        try:
            await ensure_database_exists(config.postgres_dsn)
            pool = await asyncpg.create_pool(dsn=config.postgres_dsn)
        except Exception as e:
            logger.error(build_db_error_hint(config.postgres_dsn, e))
            return
    except Exception as e:
        logger.error(build_db_error_hint(config.postgres_dsn, e))
        return
    db = DB(pool)
    await db.migrate()
    RUNTIME_ADMIN_IDS.clear()
    RUNTIME_ADMIN_IDS.update(_parse_admin_ids(get_settings().admin_user_ids))
    stored_admins = await db.get_app_setting("admin_user_ids")
    if stored_admins:
        RUNTIME_ADMIN_IDS.update(_parse_admin_ids(stored_admins))
    logger.info("База данных готова.")
    grizzly = GrizzlyClient(config.grizzly_api_key, config.grizzly_base_url)
    crypto = CryptoPayClient(config.crypto_pay_token, config.crypto_pay_base_url)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
@dp.callback_query(F.data == "sub:check")
async def sub_check(cb: CallbackQuery):
    ok, missing = await has_required_subscriptions(cb.from_user.id)
    if not ok:
        await cb.message.answer(
            "Вы ещё не подписались на все каналы.\nПожалуйста, подпишитесь и нажмите ещё раз.",
            reply_markup=required_subs_kb(missing),
        )
        await cb.answer("Проверка не пройдена", show_alert=True)
        return
    VERIFIED_SUB_USERS.add(cb.from_user.id)
    await db.mark_subscription_verified(cb.from_user.id)
    me = await db.user(cb.from_user.id)
    await cb.message.answer_photo(
        photo="https://imgur.gg/f/ArQ0tkg",
        caption=(
            "Привет, пользователь! 👋\n"
            "Добро пожаловать в наш SMS-бот. Выбирай, что хочешь.\n\n"
            "<b>Что умеет бот:</b>\n"
            "• Покупка виртуальных номеров по странам и сервисам\n"
            "• Выбор количества и подтверждение покупки\n"
            "• Автополлинг SMS-кодов\n"
            "• Пополнение баланса и история заказов\n"
            "• Поддержка и профиль\n\n"
            f"Текущий баланс: <b>{Decimal(str(me['balance'])):.2f}</b> USDT"
        ),
        reply_markup=main_kb(cb.from_user.id),
    )
    await cb.answer("Доступ открыт")ы
