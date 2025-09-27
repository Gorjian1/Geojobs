import os, sys, asyncio, json, math
from datetime import datetime, timezone
from dateutil import tz
import psycopg
from psycopg.rows import dict_row

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from telethon.tl.types import Message

# ---------- utils ----------
def log(msg: str):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_utc} UTC] {msg}", flush=True)

def env(name: str, required=True, default=None):
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        log(f"❌ ENV {name} is missing"); sys.exit(1)
    return v

async def aiter_messages_safe(client: TelegramClient, peer, min_id: int):
    """Итерация сообщений с защитой от FloodWait."""
    try:
        async for m in client.iter_messages(peer, min_id=min_id):
            yield m
    except FloodWaitError as e:
        wait_s = int(e.seconds) + 1
        log(f"⏳ FloodWaitError: sleep {wait_s}s"); await asyncio.sleep(wait_s)

def message_to_attachments(m: Message):
    ent = [type(e).__name__ for e in (m.entities or [])]
    return {
        "has_media": bool(m.media),
        "media_type": type(m.media).__name__ if m.media else None,
        "entities": ent,
        "reply_to_msg_id": getattr(getattr(m, "reply_to", None), "reply_to_msg_id", None),
        "fwd_from": bool(getattr(m, "fwd_from", None))
    }

# ---------- DB ops ----------
def get_conn():
    db_url = env("DATABASE_URL")
    return psycopg.connect(db_url, row_factory=dict_row)

def ensure_source(cur, kind: str, name: str, url: str) -> int:
    cur.execute("""
        select id from public.sources
        where kind=%s and coalesce(url,'')=%s
        limit 1
    """, (kind, url))
    r = cur.fetchone()
    if r: return r["id"]
    cur.execute("""
        insert into public.sources (kind, name, url)
        values (%s, %s, %s) returning id
    """, (kind, name, url))
    return cur.fetchone()["id"]

def max_msg_id(cur, source_id: int) -> int:
    # external_id хранится как text, но для телеги это число — кастуем
    cur.execute("""
      select coalesce(max((case when external_id ~ '^[0-9]+$' then external_id::bigint else null end)), 0) as max_id
      from public.raw_items where source_id=%s
    """, (source_id,))
    return int(cur.fetchone()["max_id"] or 0)

def insert_raw(cur, source_id: int, msg: Message) -> int | None:
    attachments = json.dumps(message_to_attachments(msg))
    cur.execute("""
      insert into public.raw_items
        (source_id, external_id, published_at, author, url, text_raw, attachments)
      values
        (%s, %s, %s, %s, %s, %s, %s)
      on conflict (source_id, external_id) do nothing
      returning id
    """, (
        source_id,
        str(msg.id),
        msg.date.replace(tzinfo=timezone.utc),
        str(getattr(msg, "sender_id", "")),
        None,  # можно собрать t.me/<username>/<id> ниже, если нужно
        msg.message or "",
        attachments
    ))
    row = cur.fetchone()
    return row["id"] if row else None

# ---------- Telegram ingest ----------
async def ingest_telegram():
    api_id = int(env("TG_API_ID"))
    api_hash = env("TG_API_HASH")
    string_session = env("TG_STRING_SESSION")
    channels_raw = env("TG_CHANNELS", required=False, default="")
    channels = [c.strip() for c in channels_raw.split(",") if c.strip()]

    if not channels:
        log("⚠️  TG_CHANNELS is empty. Add comma-separated list of channels/links.")
        return

    log(f"🔌 Connecting to Telegram (channels: {len(channels)})")
    async with TelegramClient(StringSession(string_session), api_id, api_hash) as client:
        # DB connection inside to keep session short
        with get_conn() as conn:
            with conn.cursor() as cur:
                total_new = 0
                for ch in channels:
                    try:
                        entity = await client.get_entity(ch)
                        # human-readable name/url
                        ch_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(ch)
                        ch_url = f"https://t.me/{getattr(entity, 'username', '')}" if getattr(entity, "username", None) else str(ch)

                        source_id = ensure_source(cur, "telegram", ch_title, ch_url)
                        last_id = max_msg_id(cur, source_id)
                        log(f"📥 {ch_title}: fetching messages > {last_id}")

                        new_cnt = 0
                        async for m in aiter_messages_safe(client, entity, min_id=last_id):
                            if not isinstance(m, Message):
                                continue
                            # фильтруем системные/пустые
                            if not (m.message or m.media):
                                continue
                            inserted = insert_raw(cur, source_id, m)
                            if inserted:
                                new_cnt += 1
                                if new_cnt % 50 == 0:
                                    log(f"… {ch_title}: inserted {new_cnt}")
                        conn.commit()
                        total_new += new_cnt
                        log(f"✅ {ch_title}: +{new_cnt} new")
                    except Exception as e:
                        log(f"💥 Error on {ch}: {e}")
                        conn.rollback()
                log(f"🎯 Telegram ingest done. New raw_items: {total_new}")

    # timestamp in your local tz
    ams = datetime.now(timezone.utc).astimezone(tz.gettz("Europe/Amsterdam"))
    log(f"🕒 Finished. Local time (Amsterdam): {ams.strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    asyncio.run(ingest_telegram())

if __name__ == "__main__":
    try:
        log("🚀 Ingest job started")
        main()
    except Exception as e:
        log(f"💥 FATAL: {e}")
        raise
