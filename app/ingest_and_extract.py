"""
Ingest & Extract (Telegram → Postgres, RAW stage)

ENV:
  - DATABASE_URL         : postgres connection string (Supabase/Neon), e.g. postgres://.../?sslmode=require
  - TG_API_ID            : Telegram api_id (int)
  - TG_API_HASH          : Telegram api_hash (str)
  - TG_STRING_SESSION    : Telethon StringSession (created локально)
  - TG_CHANNELS          : comma/newline separated list of channels/links (e.g. "https://t.me/foo, https://t.me/bar")

Tables expected (created earlier):
  public.sources(kind,name,url,external_id,...)
  public.raw_items(source_id,external_id,published_at,author,url,text_raw,attachments,... unique (source_id, external_id))
"""

import os, sys, asyncio, json
from typing import Optional, AsyncIterator
from datetime import datetime, timezone
from dateutil import tz

import psycopg
from psycopg.rows import dict_row

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError
from telethon.tl.types import Message


# ----------------------------- logging & env ----------------------------- #

def log(msg: str) -> None:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_utc} UTC] {msg}", flush=True)

def env(name: str, required: bool = True, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        log(f"❌ ENV {name} is missing")
        sys.exit(1)
    return val  # type: ignore


# ----------------------------- helpers ----------------------------- #

def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def split_channels(raw: str) -> list[str]:
    # supports comma, newline, spaces
    if not raw:
        return []
    parts: list[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        s = chunk.strip()
        if s:
            parts.append(s)
    return parts

async def aiter_messages_safe(client: TelegramClient, peer, min_id: int) -> AsyncIterator[Message]:
    """Iterate messages with basic FloodWait protection."""
    try:
        async for m in client.iter_messages(peer, min_id=min_id):
            yield m  # type: ignore
    except FloodWaitError as e:
        wait_s = int(getattr(e, "seconds", 5)) + 1
        log(f"⏳ FloodWaitError: sleeping {wait_s}s")
        await asyncio.sleep(wait_s)

def message_to_attachments(m: Message) -> dict:
    entities = [type(e).__name__ for e in (m.entities or [])]
    return {
        "has_media": bool(m.media),
        "media_type": type(m.media).__name__ if m.media else None,
        "entities": entities,
        "reply_to_msg_id": getattr(getattr(m, "reply_to", None), "reply_to_msg_id", None),
        "fwd_from": bool(getattr(m, "fwd_from", None)),
    }

def build_message_url(entity, msg_id: int) -> Optional[str]:
    uname = getattr(entity, "username", None)
    if uname:
        return f"https://t.me/{uname}/{msg_id}"
    return None


# ----------------------------- DB ops ----------------------------- #

def get_conn():
    db_url = env("DATABASE_URL")
    return psycopg.connect(db_url, row_factory=dict_row)

def ensure_source(cur, kind: str, name: str, url: str) -> int:
    cur.execute(
        """
        select id from public.sources
        where kind = %s and coalesce(url,'') = %s
        limit 1
        """,
        (kind, url),
    )
    row = cur.fetchone()
    if row:
        return row["id"]
    cur.execute(
        """
        insert into public.sources (kind, name, url)
        values (%s, %s, %s)
        returning id
        """,
        (kind, name, url),
    )
    return cur.fetchone()["id"]

def max_msg_id(cur, source_id: int) -> int:
    cur.execute(
        """
        select coalesce(max((case when external_id ~ '^[0-9]+$' then external_id::bigint else null end)), 0) as max_id
        from public.raw_items
        where source_id = %s
        """,
        (source_id,),
    )
    return int(cur.fetchone()["max_id"] or 0)

def insert_raw(cur, source_id: int, msg: Message, msg_url: Optional[str]) -> Optional[int]:
    attachments = json.dumps(message_to_attachments(msg))
    published_at = ensure_utc(msg.date)
    author = str(getattr(msg, "sender_id", "") or "")  # 채널 посты могут быть без sender_id
    text_raw = msg.message or ""
    cur.execute(
        """
        insert into public.raw_items
            (source_id, external_id, published_at, author, url, text_raw, attachments)
        values
            (%s, %s, %s, %s, %s, %s, %s)
        on conflict (source_id, external_id) do nothing
        returning id
        """,
        (source_id, str(msg.id), published_at, author, msg_url, text_raw, attachments),
    )
    r = cur.fetchone()
    return r["id"] if r else None


# ----------------------------- Telegram ingest ----------------------------- #

async def ingest_telegram() -> None:
    api_id = int(env("TG_API_ID"))
    api_hash = env("TG_API_HASH")
    string_session = env("TG_STRING_SESSION")
    channels_conf = env("TG_CHANNELS", required=False, default="")
    channels = split_channels(channels_conf)

    if not channels:
        log("⚠️  TG_CHANNELS is empty. Add comma/newline separated list of channels.")
        return

    log(f"🔌 Connecting to Telegram (channels: {len(channels)})")
    client = TelegramClient(StringSession(string_session), api_id, api_hash)
    await client.connect()

    # Никаких интерактивных логинов на раннере!
    if not await client.is_user_authorized():
        log("❌ TG_STRING_SESSION не авторизована. Пересоздай строку сессии локально тем же api_id/api_hash и обнови секрет.")
        await client.disconnect()
        return

    me = await client.get_me()
    log(f"🙋 Authorized as: id={getattr(me, 'id', '?')} username={getattr(me, 'username', '') or '—'}")

    total_new = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for ch in channels:
                try:
                    entity = await client.get_entity(ch)
                except (ChannelPrivateError, UsernameNotOccupiedError) as e:
                    log(f"🚫 Cannot access {ch}: {e}")
                    continue
                except Exception as e:
                    log(f"💥 get_entity failed for {ch}: {e}")
                    continue

                ch_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(ch)
                ch_url = f"https://t.me/{getattr(entity, 'username', '')}" if getattr(entity, "username", None) else str(ch)

                # ensure source & get last processed id
                source_id = ensure_source(cur, "telegram", ch_title, ch_url)
                last_id = max_msg_id(cur, source_id)
                log(f"📥 {ch_title}: fetching messages > {last_id}")

                new_cnt = 0
                try:
                    async for m in aiter_messages_safe(client, entity, min_id=last_id):
                        if not isinstance(m, Message):
                            continue
                        if not (m.message or m.media):
                            continue
                        msg_url = build_message_url(entity, m.id)
                        inserted_id = insert_raw(cur, source_id, m, msg_url)
                        if inserted_id:
                            new_cnt += 1
                            # логируем прогресс не слишком часто
                            if new_cnt % 50 == 0:
                                log(f"… {ch_title}: inserted {new_cnt}")
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    log(f"💥 Error while iterating {ch_title}: {e}")
                    continue

                total_new += new_cnt
                log(f"✅ {ch_title}: +{new_cnt} new")

    await client.disconnect()
    log(f"🎯 Telegram ingest done. New raw_items: {total_new}")

    # Для наглядности — локальное время (Europe/Amsterdam)
    ams = datetime.now(timezone.utc).astimezone(tz.gettz("Europe/Amsterdam"))
    log(f"🕒 Finished. Local time (Amsterdam): {ams.strftime('%Y-%m-%d %H:%M:%S')}")


# ----------------------------- entrypoint ----------------------------- #

def main() -> None:
    asyncio.run(ingest_telegram())

if __name__ == "__main__":
    try:
        log("🚀 Ingest job started")
        main()
    except Exception as e:
        log(f"💥 FATAL: {e}")
        raise
