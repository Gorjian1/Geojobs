import os, sys, json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
import psycopg
from psycopg.rows import dict_row


def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} UTC] {msg}", flush=True)

def env(name: str, required=True, default=None) -> str:
    v = os.getenv(name, default)
    if required and (v is None or v == ""):
        log(f"❌ ENV {name} is missing"); sys.exit(1)
    return v


# ---------------- DB helpers ----------------

def db():
    return psycopg.connect(env("DATABASE_URL"), row_factory=dict_row)

def ensure_source(cur, chat: Dict[str, Any]) -> int:
    chat_id = str(chat["id"])
    name = chat.get("title") or chat.get("username") or chat_id
    url = f"https://t.me/{chat.get('username')}" if chat.get("username") else ""
    cur.execute("select id from public.sources where kind='telegram' and external_id=%s limit 1", (chat_id,))
    r = cur.fetchone()
    if r: return r["id"]
    cur.execute(
        "insert into public.sources (kind,name,url,external_id) values ('telegram', %s, %s, %s) returning id",
        (name, url, chat_id),
    )
    return cur.fetchone()["id"]

def insert_raw(cur, source_id: int, message: Dict[str, Any], username: Optional[str]) -> Optional[int]:
    msg_id = message["message_id"]
    text = message.get("text") or message.get("caption") or ""
    if not text and not message.get("media_group_id"):
        return None

    # date приходит Unix-временем
    unix_ts = message.get("date")
    published_at = datetime.fromtimestamp(unix_ts, tz=timezone.utc) if unix_ts else datetime.now(timezone.utc)
    author = str(message.get("author_signature") or "")
    url = f"https://t.me/{username}/{msg_id}" if username else None

    attachments = {
        "has_media": any(k in message for k in ("photo", "document", "video", "audio", "sticker")),
        "media_types": [k for k in ("photo","document","video","audio","sticker") if k in message],
        "fwd_from": bool(message.get("forward_from") or message.get("forward_from_chat")),
        "entities": [e.get("type") for e in message.get("entities", [])],
        "caption_entities": [e.get("type") for e in message.get("caption_entities", [])],
    }

    cur.execute("""
      insert into public.raw_items
        (source_id, external_id, published_at, author, url, text_raw, attachments)
      values
        (%s, %s, %s, %s, %s, %s, %s)
      on conflict (source_id, external_id) do nothing
      returning id
    """, (
        source_id,
        str(msg_id),              # уникальность: (source_id, external_id)
        published_at,
        author,
        url,
        text,
        json.dumps(attachments),
    ))
    r = cur.fetchone()
    return r["id"] if r else None

def get_last_update_id(cur) -> int:
    cur.execute("select last_update_id from public.bot_state where id=1")
    row = cur.fetchone()
    return int(row["last_update_id"] or 0) if row else 0

def set_last_update_id(cur, update_id: int):
    cur.execute("update public.bot_state set last_update_id=%s, updated_at=now() where id=1", (update_id,))


# ---------------- Telegram Bot API ----------------

def tg_get_updates(token: str, offset: int) -> List[Dict[str, Any]]:
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params = {
        "offset": offset,
        "limit": 100,
        "timeout": 0,
        # "allowed_updates": json.dumps(["channel_post"])  # можно указать, но не обязательно
    }
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"getUpdates not ok: {data}")
    return data.get("result", [])


def main():
    log("🚀 Bot poll started")
    token = env("TG_BOT_TOKEN")
    allowed = [s.strip() for s in os.getenv("ALLOWED_CHAT_IDS", "").replace("\n", ",").split(",") if s.strip()]
    allowed_set = set(allowed) if allowed else None
    if allowed_set:
        log(f"🔒 ALLOWED_CHAT_IDS set: {', '.join(allowed)}")
    else:
        log("🔓 ALLOWED_CHAT_IDS not set — примем все channel_post и выведем chat_id в логах.")

    with db() as conn:
        with conn.cursor() as cur:
            offset = get_last_update_id(cur) + 1
            updates = tg_get_updates(token, offset)
            if not updates:
                log("😴 No new updates"); return

            total_new = 0
            max_update_id = 0

            for upd in updates:
                max_update_id = max(max_update_id, int(upd["update_id"]))

                msg = upd.get("channel_post")
                if not msg:
                    # игнорируем лички/группы/инлайн и т.п.
                    continue

                chat = msg["chat"]  # {'id':..., 'title':..., 'type':'channel', 'username':?}
                if chat.get("type") != "channel":
                    continue

                chat_id = str(chat["id"])
                if allowed_set and chat_id not in allowed_set:
                    log(f"↩️  Skip chat_id={chat_id} (not in ALLOWED_CHAT_IDS)")
                    continue

                # один раз в лог — чтобы узнать chat_id канала
                log(f"📡 channel_post from chat_id={chat_id} title={chat.get('title')} username={chat.get('username')}")

                source_id = ensure_source(cur, chat)
                inserted = insert_raw(cur, source_id, msg, chat.get("username"))
                if inserted:
                    total_new += 1

            if max_update_id:
                set_last_update_id(cur, max_update_id)
            conn.commit()

            log(f"✅ Done. New raw_items inserted: {total_new}. Last update_id={max_update_id}")

    log("🏁 Bot poll finished")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"💥 ERROR: {e}")
        raise
