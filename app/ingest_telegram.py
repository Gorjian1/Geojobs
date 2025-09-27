import os
import re
import asyncio
from datetime import timezone
from typing import Optional, Tuple, Any, Dict

import psycopg
from psycopg.rows import dict_row
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

DATABASE_URL = os.environ["DATABASE_URL"]
TG_API_ID = int(os.environ["TG_API_ID"])
TG_API_HASH = os.environ["TG_API_HASH"]
TG_STRING_SESSION = os.environ["TG_STRING_SESSION"]

# ---- DB helpers -------------------------------------------------------------

@retry(wait=wait_exponential_jitter(1, 5), stop=stop_after_attempt(5))
async def _get_conn():
    # psycopg 3 умеет async
    return await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True)

async def fetch_sources(conn) -> list[Dict[str, Any]]:
    q = """
    select id, url, name, external_id
    from public.sources
    where kind = 'telegram'
    order by id;
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(q)
        return await cur.fetchall()

async def get_cursor(conn, source_id: int) -> Optional[int]:
    q = "select cursor_text from public.ingest_cursors where source_id = %s"
    async with conn.cursor() as cur:
        await cur.execute(q, (source_id,))
        row = await cur.fetchone()
        if not row or not row[0]:
            return None
        try:
            return int(row[0])
        except ValueError:
            return None

async def set_cursor(conn, source_id: int, message_id: int):
    q = """
    insert into public.ingest_cursors (source_id, cursor_text, updated_at)
    values (%s, %s, now())
    on conflict (source_id) do update
      set cursor_text = excluded.cursor_text, updated_at = now();
    """
    async with conn.cursor() as cur:
        await cur.execute(q, (source_id, str(message_id)))

async def upsert_source_external_id(conn, source_id: int, external_id: str):
    q = "update public.sources set external_id = %s where id = %s"
    async with conn.cursor() as cur:
        await cur.execute(q, (external_id, source_id))

async def insert_raw_item(conn, source_id: int, m: Message, url_guess: Optional[str]):
    q = """
    insert into public.raw_items
      (source_id, external_id, published_at, fetched_at, author, url, text_raw, attachments)
    values
      (%s, %s, %s, now(), %s, %s, %s, %s)
    on conflict (source_id, external_id) do nothing
    """
    text = m.message or ""
    author = str(m.sender_id) if getattr(m, "sender_id", None) else None

    # attachments: просто типы; файлы мы не качаем на этом этапе
    attach = []
    if m.media:
        attach.append(type(m.media).__name__)

    attachments_json = psycopg.types.json.Jsonb(attach)

    async with conn.cursor() as cur:
        await cur.execute(
            q,
            (
                source_id,
                str(m.id),                        # external_id = message_id
                m.date.astimezone(timezone.utc),  # published_at
                author,
                url_guess,
                text,
                attachments_json,
            ),
        )

# ---- Telegram helpers -------------------------------------------------------

_username_re = re.compile(r"https?://t\.me/(@?)([A-Za-z0-9_]+)/?$")

async def resolve_entity_and_url_hint(client: TelegramClient, source_url: str) -> Tuple[Any, Optional[str]]:
    """
    Вернём entity для Telethon и шаблон публичной ссылки 'https://t.me/<username>'
    (если у канала есть username). Для приватных вернётся None как url_hint.
    """
    m = _username_re.match(source_url.strip())
    entity = await client.get_entity(source_url)
    url_hint = None
    if m:
        username = m.group(2).lstrip("@")
        url_hint = f"https://t.me/{username}"
    return entity, url_hint

def message_public_url(url_hint: Optional[str], message_id: int) -> Optional[str]:
    if not url_hint:
        return None
    return f"{url_hint}/{message_id}"

# ---- Main ingest ------------------------------------------------------------

async def ingest_telegram():
    print("🔎 Starting Telegram ingest...")
    conn = await _get_conn()
    sources = await fetch_sources(conn)
    if not sources:
        print("ℹ️ Нет источников kind=telegram в таблице sources. Добавь их и перезапусти.")
        await conn.close()
        return

    async with TelegramClient(StringSession(TG_STRING_SESSION), TG_API_ID, TG_API_HASH) as client:
        for s in sources:
            sid, surl, sname, s_ext_id = s["id"], s["url"], s["name"], s["external_id"]
            print(f"— Обрабатываю [{sid}] {sname} :: {surl}")

            try:
                entity, url_hint = await resolve_entity_and_url_hint(client, surl)
                # сохраним numeric channel id в external_id (для удобства дедупа/отладки)
                try:
                    await upsert_source_external_id(conn, sid, str(entity.id))
                except Exception:
                    pass

                min_id = await get_cursor(conn, sid)
                # Первую историю ограничим, чтобы не «утонуть»
                # Если курсора нет — возьмём только последние 200 сообщений
                limit_first = 200 if min_id is None else None

                newest_seen = min_id or 0
                async for msg in client.iter_messages(entity, min_id=min_id, reverse=True, limit=limit_first):
                    if not isinstance(msg, Message):
                        continue
                    url = message_public_url(url_hint, msg.id)
                    await insert_raw_item(conn, sid, msg, url)
                    if msg.id > newest_seen:
                        newest_seen = msg.id

                if newest_seen and newest_seen != (min_id or 0):
                    await set_cursor(conn, sid, newest_seen)
                    print(f"  ✔ Обновил курсор: {min_id} → {newest_seen}")
                else:
                    print("  ✔ Новых сообщений нет")

            except Exception as e:
                print(f"  ⚠️ Ошибка на источнике {sname}: {e}")

    await conn.close()
    print("✅ Telegram ingest завершён")

if __name__ == "__main__":
    asyncio.run(ingest_telegram())
