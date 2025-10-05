import os, json, time, re
from typing import Any, Dict, List
from dotenv import load_dotenv
import httpx
from supabase import create_client, Client
from rich import print as rprint

load_dotenv()

# Ollama
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.0"))

# Supabase
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Tables / columns
RAW_TABLE = os.getenv("RAW_TABLE", "raw_messages")
RAW_ID_COLUMN = os.getenv("RAW_ID_COLUMN", "id")
RAW_TEXT_COLUMN = os.getenv("RAW_TEXT_COLUMN", "text")
PARSED_TABLE = os.getenv("PARSED_TABLE", "parsed_jobs")

# Optional: pass-through metadata columns from RAW->PARSED (flattened)
# e.g.: METADATA_COLUMNS="source_id,external_id,author,url,published_at,fetched_at,attachments"
METADATA_COLUMNS = [c.strip() for c in os.getenv("METADATA_COLUMNS", "").split(",") if c.strip()]

# Worker
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "16"))
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", ".checkpoint")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def read_checkpoint() -> int:
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return -1  # nothing processed yet

def write_checkpoint(val: int):
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        f.write(str(val))

def build_prompt(text: str) -> str:
    from pathlib import Path

    system_prompt = Path(__file__).with_name("prompt.md").read_text(encoding="utf-8")
    # Склеиваем частями, без f-строк и бэкслешей:
    parts = [
        system_prompt,
        "",
        "Текст:",
        '"""',
        text or "",
        '"""',
    ]
    return "\n".join(parts)


def call_ollama(prompt: str) -> Dict[str, Any]:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "format": "json",
        "options": {"temperature": TEMPERATURE}
    }
    url = f"{OLLAMA_HOST}/api/generate"
    out = ""
    with httpx.stream("POST", url, json=payload, timeout=180.0) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            if "response" in data:
                out += data["response"]
            if data.get("done"):
                break
    out = out.strip()
    out = re.sub(r"^```json\s*|```$", "", out, flags=re.IGNORECASE|re.MULTILINE).strip()
    return json.loads(out)

def fetch_batch_after(last_id: int) -> List[dict]:
    # Select only the necessary columns
    cols = [RAW_ID_COLUMN, RAW_TEXT_COLUMN] + METADATA_COLUMNS
    col_expr = ",".join(cols)
    q = (
        supabase.table(RAW_TABLE)
        .select(col_expr)
        .gt(RAW_ID_COLUMN, last_id)
        .order(RAW_ID_COLUMN, desc=False)
        .limit(BATCH_SIZE)
    )
    res = q.execute()
    return res.data or []

def upsert_parsed(raw_row: dict, parsed: Dict[str, Any]):
    record = {"raw_id": raw_row[RAW_ID_COLUMN], **parsed}
    for c in METADATA_COLUMNS:
        if c in raw_row:
            record[c] = raw_row[c]
    supabase.table(PARSED_TABLE).upsert(record, on_conflict="raw_id").execute()

def main():
    rprint(f"[bold]Using model[/]: {OLLAMA_MODEL} at {OLLAMA_HOST}")
    rprint(f"[bold]Reading from[/]: {RAW_TABLE} ({RAW_ID_COLUMN}, {RAW_TEXT_COLUMN})")
    rprint(f"[bold]Metadata passthrough[/]: {METADATA_COLUMNS if METADATA_COLUMNS else '—'}")
    last_id = read_checkpoint()
    rprint(f"[bold]Checkpoint[/]: {last_id}")

    while True:
        batch = fetch_batch_after(last_id)
        if not batch:
            rprint("[green]No new messages. Sleeping 10s...[/]")
            time.sleep(10)
            continue

        for row in batch:
            rid = row[RAW_ID_COLUMN]
            text = (row.get(RAW_TEXT_COLUMN) or "").strip()
            if not text:
                rprint(f"[yellow]Skip empty text[/] id={rid}")
                last_id = rid
                write_checkpoint(last_id)
                continue
            try:
                parsed = call_ollama(build_prompt(text))
                parsed.setdefault("source_text", text)
                upsert_parsed(row, parsed)
                rprint(f"[cyan]Parsed[/] id={rid} ✓")
            except Exception as e:
                rprint(f"[red]Failed[/] id={rid}: {e}")
            finally:
                last_id = rid
                write_checkpoint(last_id)
        time.sleep(1)

if __name__ == "__main__":
    main()
