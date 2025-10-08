#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, Field, model_validator
from rapidfuzz import fuzz, process
from supabase import Client, create_client  # type: ignore
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

# информативные ошибки Supabase (опционально)
try:
    from postgrest import APIError as PostgrestAPIError  # type: ignore
except Exception:  # pragma: no cover
    PostgrestAPIError = Exception  # type: ignore

# -------- pretty console (rich) --------
HAS_RICH = True
try:
    from rich.console import Console
    from rich.progress import (
        Progress,
        SpinnerColumn,
        BarColumn,
        TextColumn,
        TimeElapsedColumn,
        TimeRemainingColumn,
    )
    from rich.traceback import install as rich_traceback_install
except Exception:  # pragma: no cover
    HAS_RICH = False
    Console = None  # type: ignore
    Progress = None  # type: ignore

    class _Stub:  # noqa: N801
        def __call__(self, *a, **k):  # pragma: no cover
            return None

    SpinnerColumn = BarColumn = TextColumn = TimeElapsedColumn = TimeRemainingColumn = _Stub()  # type: ignore

    def rich_traceback_install(**kwargs):
        return None

# ---------------- ENV загрузка ----------------
# 1) корневой .env (относительно cwd); 2) .env рядом со скриптом (если вдруг есть), но НЕ перетираем уже считанное.
load_dotenv(find_dotenv(filename=".env", usecwd=True), override=True)
load_dotenv(Path(__file__).with_name(".env"), override=False)

rich_traceback_install(show_locals=False)

# ---------------- Утилиты ----------------
def pretty_print(msg: str) -> None:
    if (os.getenv("PRETTY", "1") != "0") and HAS_RICH and Console:
        Console().print(msg)  # type: ignore
    else:
        print(re.sub(r"\[/?[a-z]+\]", "", msg))


def env_get(*names: str, default: Optional[str] = None) -> Optional[str]:
    """Вернуть первое непустое значение из ENV по списку имён (со strip())."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default



# ---------------- Конфиг ----------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = env_get("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_KEY")
RAW_TABLE = os.getenv("SUPABASE_RAW_TABLE", "v_jobs_input")

PARSED_TABLE = os.getenv("SUPABASE_PARSED_TABLE", "jobs")
RAW_TEXT_FIELD = os.getenv("RAW_TEXT_FIELD", "text_raw")

# failover: сначала облако, затем локально
CLOUD_MODEL = env_get("OLLAMA_CLOUD_MODEL", "CLOUD_MODEL")
CLOUD_HOST = env_get("OLLAMA_CLOUD_HOST", "CLOUD_HOST", default="https://ollama.com").rstrip("/")
CLOUD_API_KEY = env_get("OLLAMA_CLOUD_API_KEY", "CLOUD_API_KEY")

LOCAL_MODEL = env_get("OLLAMA_MODEL", "LLM_MODEL", "MODEL", default="qwen2.5:7b-instruct")
LOCAL_HOST = env_get("OLLAMA_HOST", "LLM_HOST", default="http://127.0.0.1:11434").rstrip("/")

# thinking-валидатор (опционально)
ENABLE_VALIDATOR = os.getenv("ENABLE_VALIDATOR", "1") != "0"
VALIDATOR_MODEL = os.getenv("VALIDATOR_MODEL", "")
VALIDATOR_HOST = env_get("VALIDATOR_HOST", default=LOCAL_HOST).rstrip("/")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))
USE_PRETTY = (os.getenv("PRETTY", "1") != "0") and HAS_RICH

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("⛔ Нужны SUPABASE_URL и SUPABASE_SERVICE_ROLE_KEY в .env")

# ---------------- Supabase client ----------------
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------- Pydantic схемы ----------------
class Salary(BaseModel):
    min: Optional[int] = None
    max: Optional[int] = None
    currency: str = "unknown"
    period: str = "unknown"  # month|day|hour|shift|rotation|project|unknown

    @model_validator(mode="after")
    def _fix_range(self):
        if self.min is not None and self.max is not None and self.min > self.max:
            self.min, self.max = self.max, self.min
        return self


class City(BaseModel):
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None


class Contact(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    telegram: Optional[str] = None
    whatsapp: Optional[str] = None
    link: Optional[str] = None


class SourceInfo(BaseModel):
    platform: Optional[str] = None
    post_id: Optional[str] = None
    author_id: Optional[str] = None
    posted_at: Optional[str] = None


class ParsedItem(BaseModel):
    role: str = Field(default="unknown")  # employer|candidate|unknown
    position: Optional[str] = None
    salary: Salary = Field(default_factory=Salary)
    employment: List[str] = Field(default_factory=list)
    schedule: List[str] = Field(default_factory=list)
    equipment: List[str] = Field(default_factory=list)
    skills: List[str] = Field(default_factory=list)
    experience_years: Optional[float] = None
    city: City = Field(default_factory=City)
    contact: Contact = Field(default_factory=Contact)
    source: SourceInfo = Field(default_factory=SourceInfo)
    text_clean: str = ""
    confidence: float = 0.0
    errors: List[str] = Field(default_factory=list)


# ---------------- Санитайзинг & нормализация ----------------
FORWARDED_RE = re.compile(r"^переслано от.*$", re.I | re.M)
HASHTAG_RE = re.compile(r"(?:^|\s)#[\wА-Яа-я_]+")
MULTISPACE_RE = re.compile(r"[ \t]{2,}")
URL_RE = re.compile(r"https?://\S+")

PHONE_RE = re.compile(r"\+?\d[\d\s\-\.\(\)]{7,}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
TG_RE = re.compile(r"@([A-Za-z0-9_]{4,})")

def sanitize_text(t: str) -> str:
    if not t:
        return t
    t = FORWARDED_RE.sub("", t)
    t = HASHTAG_RE.sub("", t)
    t = URL_RE.sub("", t)
    t = MULTISPACE_RE.sub(" ", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t

CANON_EMPLOYMENT = {
    "полная занятость": "full_time",
    "full time": "full_time",
    "full-time": "full_time",
    "частичная": "part_time",
    "part time": "part_time",
    "вахта": "rotation",
    "ротация": "rotation",
    "смена": "shift",
    "контракт": "contract",
    "стажировка": "internship",
    "temporary": "temporary",
}

CANON_SCHEDULE = {
    "вахта": "вахта",
    "ротация": "вахта",
    "2/2": "смена",
    "15/15": "вахта",
    "30/30": "вахта",
    "60/30": "вахта",
    "45/15": "вахта",
    "сменный график": "смена",
    "5/2": "5/2",
    "удаленно": "удаленно",
    "полевая": "полевая",
    "гибкий": "гибкий",
}

CANON_EQUIP = ["GNSS", "GPS", "RTK", "Тахеометр", "Нивелир", "Дрон", "БПЛА", "Лазерный сканер", "ГИС", "QGIS", "Civil 3D", "Total Station"]
CANON_SKILLS = ["AutoCAD", "Civil 3D", "Revit", "QGIS", "ArcGIS", "Topo", "CAD", "Python", "SQL", "Metashape", "Photogrammetry"]

CURRENCY_HINTS = {"₽": "RUB", "руб": "RUB", "т.р": "RUB", "тыс": "RUB", "KZT": "KZT", "₸": "KZT", "тенге": "KZT", "$": "USD", "USD": "USD", "дол": "USD", "€": "EUR", "EUR": "EUR"}
SALARY_PERIOD_HINTS = {"/ч": "hour", "в час": "hour", "час": "hour", "/д": "day", "в день": "day", "смена": "shift", "в месяц": "month", "месяц": "month", "мес": "month", "м/ц": "month", "вахта": "rotation", "за проект": "project"}

def _canon_list(values: List[str], universe: List[str], limit: int = 8) -> List[str]:
    out: List[str] = []
    for v in (values or []):
        v = (v or "").strip()
        if not v:
            continue
        res: Optional[Tuple[str, float, Any]] = process.extractOne(v, universe, scorer=fuzz.WRatio)
        if res:
            best, score, *_ = res
            out.append(best if score >= 80 else v)
        else:
            out.append(v)
        if len(out) >= limit:
            break
    seen = set(); uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x); uniq.append(x)
    return uniq

def normalize(parsed: ParsedItem) -> ParsedItem:
    if parsed.employment:
        mapped = []
        for e in parsed.employment:
            el = (e or "").lower()
            mapped.append(CANON_EMPLOYMENT.get(el, el))
        parsed.employment = list(dict.fromkeys(mapped))
    if parsed.schedule:
        mapped = []
        for s in parsed.schedule:
            sl = (s or "").lower()
            mapped.append(CANON_SCHEDULE.get(sl, sl))
        parsed.schedule = list(dict.fromkeys(mapped))
    parsed.equipment = _canon_list(parsed.equipment, CANON_EQUIP)
    parsed.skills = _canon_list(parsed.skills, CANON_SKILLS)
    if parsed.salary.currency not in {"RUB", "KZT", "USD", "EUR", "OTHER", "unknown"}:
        parsed.salary.currency = "OTHER"
    if parsed.salary.period not in {"month","day","hour","shift","rotation","project","unknown"}:
        parsed.salary.period = "unknown"
    try:
        parsed.confidence = max(0.0, min(1.0, float(parsed.confidence or 0)))
    except Exception:
        parsed.confidence = 0.0
    return parsed

# ---------------- Энрихер ----------------
RE_TAG_RESUME = re.compile(r"#\s?(резюме|камеральщик)\b", re.I)
RE_TAG_VAC = re.compile(r"#\s?(вакансия|работа)\b", re.I)
RE_NEED = re.compile(r"\b(требуется|ищем|в компанию|открыт набор)\b", re.I)
RE_OFFER_SELF = re.compile(r"\b(предлагаю услуги|готов(а)? выполнить|ищу подработк|ищу удаленк)\b", re.I)

RE_SALARY_TRUB = re.compile(r"(?:з[п/\:]|зарплата|оплата)\s*[:=~-]?\s*(от\s*)?([0-9][0-9\s]{1,})(\+)?\s*(т\.?р|тыс\.?|руб\.?|₽)?", re.I)
RE_SALARY_NUM = re.compile(r"\b([12][0-9]{2}\s?[0-9]{3}|[1-9][0-9]{4,})(?:\s*₽|\s*руб|\s*р\b)?", re.I)
RE_PERIOD_MON = re.compile(r"\b(в\s*мес(яц)?|месяц|/мес)\b", re.I)
RE_PERIOD_DAY = re.compile(r"\b(в\s*день|/д|сут(ки)?)\b", re.I)
RE_PERIOD_HR = re.compile(r"\b(в\s*час|/ч)\b", re.I)
RE_PERIOD_SHFT = re.compile(r"\b(смена|за\s*смену)\b", re.I)
RE_ROTATION = re.compile(r"\b(вахта|вахтовый)\b|\b(\d{1,2}\s*/\s*\d{1,2})\b", re.I)

RE_LOC = re.compile(r"\b(астраханск(ая|ой) область|камчатка|мурманск(ая|ой) обл\.?|белокаменка|новый\s+уренгой|москва|московск(ая|ой) область|шерегеш)\b", re.I)
RE_CITY_ONLY = re.compile(r"(уренгой|белокаменка|москва|шерегеш|би[йй]ск|коломна|пермь|троицк|с(е|ё)ргиев\s+посад|щербинка)", re.I)

RE_EQUIP = re.compile(r"\b(гнсс|gnss|rtk|тахеометр|нивелир|бпла|дрон|сканер)\b", re.I)
RE_SKILL = re.compile(r"\b(autocad|civil\s*3d|qgis|arcgis|metashape|камеральк(а|и))\b", re.I)

def _period_from_text(t: str) -> str:
    if RE_PERIOD_MON.search(t): return "month"
    if RE_PERIOD_DAY.search(t): return "day"
    if RE_PERIOD_HR.search(t):  return "hour"
    if RE_PERIOD_SHFT.search(t):return "shift"
    if RE_ROTATION.search(t):   return "rotation"
    return "unknown"

def _rub_hint(t: str) -> str:
    return "RUB" if re.search(r"(₽|руб|т\.?р|тыс\.?)", t, re.I) else "unknown"

def _intify(num_str: str, unit_hint: str) -> Optional[int]:
    s = re.sub(r"\D", "", num_str or "")
    if not s: return None
    val = int(s)
    if unit_hint and re.search(r"(т\.?р|тыс\.?)", unit_hint, re.I):
        val *= 1000
    elif val < 1000:
        val *= 1000
    # телефоноподобные
    if 10 <= len(s) <= 12:
        return None
    if val < 15000 or val > 5000000:
        return None
    return val

def rule_enrich(parsed: ParsedItem, text: str) -> ParsedItem:
    t = text
    if parsed.role == "unknown":
        if RE_TAG_VAC.search(t) or RE_NEED.search(t):
            parsed.role = "employer"
        elif RE_TAG_RESUME.search(t) or RE_OFFER_SELF.search(t):
            parsed.role = "candidate"

    if not parsed.position:
        m = re.search(r"\b(инженер\s*пто|техник-?геодезист|геодезист(\s*камеральщик|\s*полевик)?|оператор\s*бпла|камеральщик)\b", t, re.I)
        if m:
            parsed.position = m.group(0).strip().title()

    if parsed.salary.min is None and parsed.salary.max is None:
        m = RE_SALARY_TRUB.search(t) or RE_SALARY_NUM.search(t)
        if m:
            span = m.span()
            phone_hit = any(not (ph.end() <= span[0] or ph.start() >= span[1]) for ph in PHONE_RE.finditer(t))
            if not phone_hit:
                base = m.group(2) if (m.lastindex and m.lastindex >= 2 and m.group(2)) else (m.group(1) if m.lastindex and m.group(1) else m.group(0))
                unit = m.group(4) if (m.lastindex and m.lastindex >= 4) else ""
                val = _intify(base, unit or "")
                if val is not None:
                    parsed.salary.min = val
                    parsed.salary.max = None
                    if parsed.salary.currency == "unknown":
                        parsed.salary.currency = _rub_hint(t)
                    if parsed.salary.period == "unknown":
                        parsed.salary.period = _period_from_text(t)

    if RE_ROTATION.search(t):
        if "rotation" not in parsed.employment:
            parsed.employment.append("rotation")
        if "вахта" not in parsed.schedule:
            parsed.schedule.append("вахта")
        for ratio in re.findall(r"\b\d{1,2}\s*/\s*\d{1,2}\b", t):
            if ratio not in parsed.schedule:
                parsed.schedule.append(ratio)

    if not (parsed.city.city or parsed.city.region):
        locs = [m.group(0) for m in RE_LOC.finditer(t)]
        if locs:
            if any(RE_CITY_ONLY.search(x) for x in locs):
                parsed.city.city = next(x for x in locs if RE_CITY_ONLY.search(x)).title()
            else:
                parsed.city.region = locs[0].title()
            if not parsed.city.country:
                parsed.city.country = "Россия"

    if RE_EQUIP.search(t) and not parsed.equipment:
        parsed.equipment = list({m.group(0).upper() for m in RE_EQUIP.finditer(t)})
    if RE_SKILL.search(t) and not parsed.skills:
        parsed.skills = [m.group(0).upper() for m in RE_SKILL.finditer(t)]

    if not (parsed.contact.phone or parsed.contact.telegram or parsed.contact.email):
        ph = PHONE_RE.search(t); tg = TG_RE.search(t); em = EMAIL_RE.search(t)
        if ph: parsed.contact.phone = ph.group(0)
        if tg: parsed.contact.telegram = f"@{tg.group(1)}"
        if em: parsed.contact.email = em.group(0)

    return normalize(parsed)

# ---------------- Подсказки и очистка ----------------
def cheap_hints(text: str) -> Dict[str, str]:
    t = text.lower()
    currency = next((v for k, v in CURRENCY_HINTS.items() if k in t), "unknown")
    period = next((v for k, v in SALARY_PERIOD_HINTS.items() if k in t), "unknown")
    return {"currency": currency, "period": period}

def clean_num(x, as_int=False):
    if x is None: return None
    try:
        y = float(x)
    except Exception:
        return None
    if not math.isfinite(y):
        return None
    if as_int:
        try: return int(y)
        except Exception: return None
    return y

def clean_str(s, maxlen=40000):
    if s is None: return None
    s = str(s)
    return s[:maxlen] if len(s) > maxlen else s

# ---------------- DB I/O ----------------
@retry(wait=wait_exponential_jitter(initial=1, max=8), stop=stop_after_attempt(5))
def fetch_batch(limit: int) -> List[Dict[str, Any]]:
    res = (
        sb.table(RAW_TABLE)
        .select("*")
        .order("published_at", desc=False)
        .order("fetched_at", desc=False)
        .limit(limit)
        .execute()
    )
    return list(res.data or [])

@retry(wait=wait_exponential_jitter(initial=1, max=6), stop=stop_after_attempt(5))
def upsert_job(raw_row: Dict[str, Any], parsed: ParsedItem):
    def join_or_none(lst: Optional[List[str]]):
        if not lst: return None
        s = ", ".join(x for x in lst if x)
        return s or None

    role_val = parsed.position or parsed.role or None
    rl = (parsed.role or "").lower()
    is_employer = True if rl == "employer" else False if rl == "candidate" else None
    posted_at = raw_row.get("published_at") or raw_row.get("fetched_at")

    rec = {
        "source_id": raw_row.get("source_id"),
        "raw_item_id": raw_row.get("raw_id"),
        "role": clean_str(role_val, 255),
        "employer_name": clean_str(parsed.contact.name, 255),
        "is_employer": is_employer,
        "description": clean_str(parsed.text_clean, 40000),
        "contact_telegram": clean_str(parsed.contact.telegram, 255),
        "contact_phone": clean_str(parsed.contact.phone, 255),
        "contact_email": clean_str(parsed.contact.email, 255),
        "salary_min": clean_num(parsed.salary.min, as_int=True),
        "salary_max": clean_num(parsed.salary.max, as_int=True),
        "salary_currency": clean_str(parsed.salary.currency, 16),
        "salary_period": clean_str(parsed.salary.period, 16),
        "employment_type": clean_str(join_or_none(parsed.employment), 255),
        "schedule_type": clean_str(join_or_none(parsed.schedule), 255),
        "city": clean_str(parsed.city.city, 255),
        "region": clean_str(parsed.city.region, 255),
        "country": clean_str(parsed.city.country, 255),
        "equipment": clean_str(join_or_none(parsed.equipment), 512),
        "software": clean_str(join_or_none(parsed.skills), 512),
        "experience": clean_num(parsed.experience_years),
        "language": None,
        "confidence": clean_num(parsed.confidence),
        "posted_at": posted_at,
    }

    import hashlib
    src = f"{rec.get('description') or ''}|{raw_row.get('author') or ''}|{rec.get('posted_at') or ''}"
    rec["dedup_hash"] = hashlib.sha1(src.encode("utf-8", "ignore")).hexdigest()

    try:
        sb.table(PARSED_TABLE).upsert(rec, on_conflict="raw_item_id").execute()
    except PostgrestAPIError as e:
        pretty_print(
            f"[red]Supabase APIError[/] code={getattr(e,'code',None)} "
            f"msg={getattr(e,'message',None)} hint={getattr(e,'hint',None)} details={getattr(e,'details',None)}"
        )
        raise

# ---------------- LLM (Ollama Cloud → Local) ----------------
SYSTEM_PROMPT = (
    "Ты — строгий экстрактор вакансий/резюме для геодезии. "
    "Твоя задача — извлечь поля и вернуть ЧИСТЫЙ JSON. "
    "Никакого текста, пояснений, комментариев или ``` — только один JSON-объект."
)

EXTRACTION_INSTRUCTION = (
    "Извлеки из текста следующие поля и верни JSON:\n"
    "role (employer|candidate|unknown), position,\n"
    "salary{min,max,currency,period},\n"
    "employment[], schedule[], equipment[], skills[],\n"
    "experience_years, city{city,region,country},\n"
    "contact{name,phone,email,telegram,whatsapp,link},\n"
    "source{platform,post_id,author_id,posted_at}, text_clean, confidence (0..1), errors[].\n\n"
    "Указания:\n"
    "— Используй #вакансия/#резюме как сигнал роли.\n"
    "— Распознавай вахтовые форматы 45/15, 60/30, 5/2.\n"
    "— 'т.р'/'тыс' = ×1000.\n"
    "— НЕ путай телефоны с зарплатой (номера начинаются с +7/8 и содержат 10–12 цифр).\n"
    "— Если данных нет — ставь null или пустой список.\n"
    "— Отвечай ЧИСТЫМ JSON без лишних символов."
)

LIMIT_HTTP_STATUSES = {401, 402, 403, 429}
LIMIT_TEXT_PATTERNS = ("limit","quota","credit","payment","billing","insufficient","not permitted","not allowed","subscription","rate limit")

async def _ollama_chat(host: str, model: str, text: str, api_key: Optional[str] = None, *, parse_json: bool = True) -> Dict[str, Any]:
    payload = {
        "model": model,
        "format": "json",
        "stream": False,
        "options": {"num_ctx": 8192, "temperature": 0},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": EXTRACTION_INSTRUCTION + "\nТекст:\n" + text},
        ],
    }
    headers = {}
    if host.startswith("https://ollama.com") and api_key:
        headers["Authorization"] = api_key  # если нужен Bearer, поменяй здесь
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(f"{host}/api/chat", json=payload, headers=headers)
        r.raise_for_status()
        if not parse_json:
            return {"ok": True}
        data = r.json()
        content = data.get("message", {}).get("content") or data.get("response")
        if not content:
            raise RuntimeError("Пустой ответ от LLM")
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r"\{[\s\S]*\}", content)
            if not m:
                raise
            return json.loads(m.group(0))

FALLBACK_ACTIVATED = False

async def cloud_preflight() -> bool:
    if not CLOUD_MODEL:
        return False
    try:
        await _ollama_chat(CLOUD_HOST, CLOUD_MODEL, '{"probe": true}', CLOUD_API_KEY, parse_json=False)
        return True
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        body = (e.response.text or "").lower()
        if (status in LIMIT_HTTP_STATUSES) or any(p in body for p in LIMIT_TEXT_PATTERNS):
            return False
        return False
    except Exception:
        return False

async def call_llm_with_failover(text: str) -> Dict[str, Any]:
    global FALLBACK_ACTIVATED

    # 1) облако (если доступно и не отключено)
    if CLOUD_MODEL and not FALLBACK_ACTIVATED:
        try:
            pretty_print(f"[dim]→ Cloud {CLOUD_MODEL}[/dim]")
            return await _ollama_chat(CLOUD_HOST, CLOUD_MODEL, text, CLOUD_API_KEY)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            body = (e.response.text or "").lower()
            if (status in LIMIT_HTTP_STATUSES) or any(p in body for p in LIMIT_TEXT_PATTERNS):
                pretty_print("[yellow]Cloud лимит/нет доступа — переключаюсь на локальную модель[/]")
                FALLBACK_ACTIVATED = True
            else:
                pretty_print(f"[yellow]Cloud ошибка {status} — переключаюсь на локальную[/]")
                FALLBACK_ACTIVATED = True
        except Exception as e:
            pretty_print(f"[yellow]Cloud недоступен ({e!r}) — локальный фолбэк[/]")
            FALLBACK_ACTIVATED = True

    # 2) локальный фолбэк
    pretty_print(f"[dim]→ Local {LOCAL_MODEL}[/dim]")
    return await _ollama_chat(LOCAL_HOST, LOCAL_MODEL, text)

# --------- THINKING VALIDATOR (опционально) ----------
VALIDATOR_SYSTEM = (
    "Ты — валидатор структурированных вакансий. "
    "Получишь исходный текст и JSON-объект. "
    "Исправь противоречия и заполни ПУСТЫЕ поля, если это однозначно следует из текста "
    "или общеизвестных соответствий (город→регион/страна РФ). Никаких выдумок. "
    "Верни ТОЛЬКО ОДИН JSON-объект."
)

VALIDATOR_USER_TMPL = (
    "Текст:\n{TEXT}\n\n"
    "Текущий JSON:\n{JSON}\n\n"
    "Правила:\n"
    "— Корректируй salary_min/max, currency/period если читается из текста.\n"
    "— Если city указан, а region/country пусты — подставь корректные значения (Россия) при российском городе.\n"
    "— Приведи employment/schedule к вахта/rotation, 45/15 и т.д., если явно указано.\n"
    "— Никакого текста, только JSON."
)

CITY_TO_REGION_COUNTRY = {
    "Москва": ("Москва", "Россия"),
    "Санкт-Петербург": ("Санкт-Петербург", "Россия"),
    "Белокаменка": ("Мурманская область", "Россия"),
    "Мурманск": ("Мурманская область", "Россия"),
    "Новый Уренгой": ("Ямало-Ненецкий АО", "Россия"),
    "Астрахань": ("Астраханская область", "Россия"),
    "Шерегеш": ("Кемеровская область", "Россия"),
}

def rule_impute_geo(p: ParsedItem) -> ParsedItem:
    c = (p.city.city or "").strip()
    if c:
        hit = CITY_TO_REGION_COUNTRY.get(c) or CITY_TO_REGION_COUNTRY.get(c.title())
        if hit:
            region, country = hit
            if not p.city.region: p.city.region = region
            if not p.city.country: p.city.country = country
    if p.city.city and not p.city.country:
        p.city.country = "Россия"
    return p

def rule_impute_schedule(p: ParsedItem, text: str) -> ParsedItem:
    if re.search(r"\b\d{1,2}\s*/\s*\d{1,2}\b", text):
        if "вахта" not in (p.schedule or []):
            p.schedule.append("вахта")
        if "rotation" not in (p.employment or []):
            p.employment.append("rotation")
    return p

def rule_fix_salary(p: ParsedItem, text: str) -> ParsedItem:
    if p.salary.currency == "unknown":
        p.salary.currency = _rub_hint(text)
    if p.salary.period == "unknown":
        p.salary.period = _period_from_text(text)
    if p.salary.min and p.salary.max and p.salary.min > p.salary.max:
        p.salary.min, p.salary.max = p.salary.max, p.salary.min
    return p

async def _validator_available() -> bool:
    if not ENABLE_VALIDATOR or not VALIDATOR_MODEL:
        return False
    # быстрый префлайт
    try:
        payload = {"model": VALIDATOR_MODEL, "format": "json", "stream": False, "messages": [{"role": "user", "content": '{"probe":true}'}]}
        headers = {}
        if VALIDATOR_HOST.startswith("https://ollama.com") and CLOUD_API_KEY:
            headers["Authorization"] = CLOUD_API_KEY
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.post(f"{VALIDATOR_HOST}/api/chat", json=payload, headers=headers)
            r.raise_for_status()
            return True
    except Exception:
        return False

async def call_validator_llm_async(parsed: ParsedItem, text: str) -> ParsedItem:
    body = {
        "model": VALIDATOR_MODEL,
        "format": "json",
        "stream": False,
        "options": {"num_ctx": 8192, "temperature": 0},
        "messages": [
            {"role": "system", "content": VALIDATOR_SYSTEM},
            {"role": "user", "content": VALIDATOR_USER_TMPL.format(TEXT=text, JSON=parsed.model_dump_json())},
        ],
    }
    headers = {}
    if VALIDATOR_HOST.startswith("https://ollama.com") and CLOUD_API_KEY:
        headers["Authorization"] = CLOUD_API_KEY
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(f"{VALIDATOR_HOST}/api/chat", json=body, headers=headers)
        r.raise_for_status()
        data = r.json()
        content = data.get("message", {}).get("content") or data.get("response")
        obj = json.loads(re.search(r"\{[\s\S]*\}", content).group(0)) if isinstance(content, str) else content
        return ParsedItem.model_validate(obj)

async def validate_and_impute(parsed: ParsedItem, text: str) -> ParsedItem:
    p = rule_impute_geo(parsed)
    p = rule_impute_schedule(p, text)
    p = rule_fix_salary(p, text)
    if await _validator_available():
        try:
            p2 = await call_validator_llm_async(p, text)
            p2 = normalize(p2)
            return p2
        except Exception as e:
            pretty_print(f"[yellow]Validator пропущен: {e!r}[/]")
            return p
    return p

# ---------------- Pipeline ----------------
async def parse_one(row: Dict[str, Any]) -> None:
    raw_text = (row.get(RAW_TEXT_FIELD) or "").strip()
    text = sanitize_text(raw_text)
    if not text:
        return
    hints = cheap_hints(text)
    hinted_text = f"{text}\n\n[meta hints] currency≈{hints['currency']}, period≈{hints['period']}"
    try:
        llm_json = await call_llm_with_failover(hinted_text)
        parsed = ParsedItem.model_validate(llm_json)
    except Exception as e:
        try:
            m = re.search(r"\{[\s\S]*\}", str(e))
            llm_json = json.loads(m.group(0)) if m else {}
        except Exception:
            llm_json = {}
        stub = {
            "role": (llm_json or {}).get("role", "unknown"),
            "position": (llm_json or {}).get("position"),
            "salary": (llm_json or {}).get("salary", {}),
            "employment": (llm_json or {}).get("employment", []),
            "schedule": (llm_json or {}).get("schedule", []),
            "equipment": (llm_json or {}).get("equipment", []),
            "skills": (llm_json or {}).get("skills", []),
            "experience_years": (llm_json or {}).get("experience_years"),
            "city": (llm_json or {}).get("city", {}),
            "contact": (llm_json or {}).get("contact", {}),
            "source": (llm_json or {}).get("source", {}),
            "text_clean": text,
            "confidence": 0.3,
            "errors": ["fallback"],
        }
        parsed = ParsedItem.model_validate(stub)

    parsed = normalize(parsed)
    parsed = rule_enrich(parsed, text)
    parsed = await validate_and_impute(parsed, text)

    if not (parsed.contact.phone or parsed.contact.email or parsed.contact.telegram):
        phone = PHONE_RE.search(text)
        email = EMAIL_RE.search(text)
        tg = TG_RE.search(text)
        if phone: parsed.contact.phone = phone.group(0)
        if email: parsed.contact.email = email.group(0)
        if tg: parsed.contact.telegram = f"@{tg.group(1)}"

    upsert_job(row, parsed)

# ---------------- Main loop ----------------
async def main_loop(once: bool = False):
    cloud_note = CLOUD_MODEL and f"cloud: [magenta]{CLOUD_MODEL}[/] @ [cyan]{CLOUD_HOST}[/]" or "cloud: off"
    local_note = f"local: [magenta]{LOCAL_MODEL}[/] @ [cyan]{LOCAL_HOST}[/]"
    val_note = f"validator: [magenta]{VALIDATOR_MODEL or 'off'}[/] @ [cyan]{VALIDATOR_HOST}[/]" if ENABLE_VALIDATOR and VALIDATOR_MODEL else "validator: off"
    pretty_print(f"[bold cyan]GeoJobs[/] • {cloud_note} • {local_note} • {val_note} • src: [green]{RAW_TABLE}[/] → [yellow]{PARSED_TABLE}[/]")
    pretty_print(f"[dim]ENV check:[/] OLLAMA_MODEL={os.getenv('OLLAMA_MODEL')}  LLM_MODEL={os.getenv('LLM_MODEL')}  MODEL={os.getenv('MODEL')}")

    global FALLBACK_ACTIVATED
    if CLOUD_MODEL and not FALLBACK_ACTIVATED:
        ok = await cloud_preflight()
        if not ok:
            pretty_print("[yellow]Cloud недоступен/лимит — заранее переключаюсь на локальную модель[/]")
            FALLBACK_ACTIVATED = True

    while True:
        try:
            batch = fetch_batch(BATCH_SIZE)
        except Exception as e:
            pretty_print(f"[red]Ошибка fetch_batch:[/] {e!r}")
            if once: break
            await asyncio.sleep(POLL_SECONDS); continue

        if not batch:
            if once:
                pretty_print("[yellow]Нет новых записей — выходим[/]")
                break
            if (os.getenv("PRETTY", "1") != "0") and HAS_RICH and Console:
                with Console().status(f"Жду новые записи из [green]{RAW_TABLE}[/]…", spinner="dots"):  # type: ignore
                    await asyncio.sleep(POLL_SECONDS)
            else:
                time.sleep(POLL_SECONDS)
            continue

        total = len(batch)
        pretty_print(f"[blue]Получен батч:[/] {total} записей")

        if (os.getenv("PRETTY", "1") != "0") and HAS_RICH and Console:
            with Progress(
                SpinnerColumn(style="cyan"),  # type: ignore
                TextColumn("[progress.description]{task.description}"),  # type: ignore
                BarColumn(),  # type: ignore
                TextColumn("{task.completed}/{task.total}"),  # type: ignore
                TimeElapsedColumn(),  # type: ignore
                TimeRemainingColumn(),  # type: ignore
                console=Console(),  # type: ignore
                transient=True,
            ) as progress:
                task = progress.add_task("Парсинг LLM…", total=total)  # type: ignore
                coros = [parse_one(r) for r in batch]
                for fut in asyncio.as_completed(coros):
                    try:
                        await fut
                    except Exception as e:
                        pretty_print(f"[red]Ошибка при парсинге:[/] {e!r}")
                    finally:
                        progress.advance(task)  # type: ignore
        else:
            coros = [parse_one(r) for r in batch]
            for fut in asyncio.as_completed(coros):
                try:
                    await fut
                except Exception as e:
                    print(f"Ошибка при парсинге: {e!r}")

        pretty_print("[green]Батч обработан[/]")
        if once: break

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # режим работы
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--once", action="store_true", help="однократный проход по батчу")
    g.add_argument("--watch", action="store_true", help="бесконечный цикл с интервалом POLL_SECONDS")
    # переопределения LLM из CLI
    parser.add_argument("--local-model")
    parser.add_argument("--cloud-model")
    parser.add_argument("--local-host")
    parser.add_argument("--cloud-host")
    args = parser.parse_args()

    # применяем CLI-оверрайды
    if args.local_model: LOCAL_MODEL = args.local_model
    if args.cloud_model: CLOUD_MODEL = args.cloud_model
    if args.local_host:  LOCAL_HOST  = args.local_host.rstrip("/")
    if args.cloud_host:  CLOUD_HOST  = args.cloud_host.rstrip("/")

    try:
        asyncio.run(main_loop(once=args.once))
    except KeyboardInterrupt:
        pretty_print("\n⏹ Остановлено пользователем")
