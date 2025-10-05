import os, sys, re, json, hashlib
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List

import psycopg
from psycopg.rows import dict_row

# ---------- utils ----------
def log(msg: str):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now} UTC] {msg}", flush=True)

def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        log(f"❌ ENV {name} is missing"); sys.exit(1)
    return v

DB_URL = env("DATABASE_URL")

CURRENCY_MAP = {
    "₽": "RUB", "руб": "RUB", "р.": "RUB", "р ": "RUB", "т.р": "RUB", "тр": "RUB", "тыс": "RUB",
    "тенге": "KZT", "тг": "KZT", "kzt": "KZT",
    "$": "USD", "usd": "USD",
    "€": "EUR", "eur": "EUR",
    "byn": "BYN", "uah": "UAH",
}
PERIOD_WORDS = {
    "month": ["в месяц", "в мес", "мес", "месяц", "/мес"],
    "shift": ["за смену", "смена", "/смен"],
    "hour":  ["в час", "час", "/час"],
    "project": ["за проект", "проект"],
}

EQUIP_WORDS = ["gnss","gps","тахеометр","теодолит","нивелир","leica","trimble","topcon","sokkia","dji","дрон","бпла"]
SOFT_WORDS  = ["autocad","civil 3d","civil3d","кредо","credо","credo","panorama","панорама","Компaс","compass","geomax"]

SEEKER_MARKERS = [
    "ищу работу", "ищу подработку", "рассмотрю предложения", "готов выйти", "резюме", "соискатель", "срочно ищу"
]
EMPLOYER_MARKERS = ["вакансия", "требуется", "нужен", "открыта позиция", "примем", "ищем"]

RE_PHONE = re.compile(r"(?:\+?\d[\s\-()]?){10,13}")
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
RE_TG    = re.compile(r"(?:@|t\.me/)([A-Za-z0-9_]{3,})")

def _find_currency(text: str) -> str:
    t = text.lower()
    for k, v in CURRENCY_MAP.items():
        if k in t:
            return v
    return "UNKNOWN"

def _find_period(text: str) -> str:
    t = text.lower()
    for p, keys in PERIOD_WORDS.items():
        if any(k in t for k in keys):
            return p
    return "unknown"

def _parse_salary(text: str) -> Tuple[Optional[float], Optional[float], str, str, str]:
    t = text.lower().replace(" ", " ").replace("\u00A0", " ")
    # числа вида 200 000–250 000 / 200-250 / 200–250 т.р / 200к
    rng = re.search(r"(\d[\d\s]{1,9})\s*[–\-]\s*(\d[\d\s]{1,9})\s*(к|k|тыс|т\.р|тр)?", t)
    single = re.search(r"(?:от|≈|~)?\s*(\d[\d\s]{2,9})\s*(к|k|тыс|т\.р|тр)?", t)

    mul = 1.0
    def to_num(s: str, suf: Optional[str]) -> float:
        x = float(re.sub(r"\s+", "", s))
        if suf and suf in ("к","k","тыс","т.р","тр"):
            return x * 1000.0
        return x

    currency = _find_currency(t)
    period = _find_period(t)
    raw = ""

    if rng:
        raw = rng.group(0)
        mn = to_num(rng.group(1), rng.group(3))
        mx = to_num(rng.group(2), rng.group(3))
        if mn > mx: mn, mx = mx, mn
        return mn, mx, currency, period, raw

    if single:
        raw = single.group(0)
        val = to_num(single.group(1), single.group(2))
        return val, val, currency, period, raw

    return None, None, currency, period, raw

def _contacts(text: str) -> Dict[str, Optional[str]]:
    phones = RE_PHONE.findall(text)
    emails = RE_EMAIL.findall(text)
    tg = RE_TG.findall(text)
    return {
        "phone": phones[0] if phones else None,
        "email": emails[0] if emails else None,
        "telegram": ("@" + tg[0]) if tg else None,
    }

def _schedule(text: str) -> str:
    t = text.lower()
    if "вахт" in t: return "вахта"
    if "командиров" in t: return "командировка"
    if "удален" in t or "remote" in t or "дистанц" in t: return "remote"
    if "офис" in t: return "офис"
    if "гибрид" in t or "hybrid" in t: return "hybrid"
    return "unknown"

def _employment(text: str) -> str:
    t = text.lower()
    if "полная" in t or "full" in t: return "full"
    if "частич" in t or "part" in t: return "part"
    if "подряд" in t or "контракт" in t or "contract" in t: return "contract"
    if "стаж" in t or "intern" in t: return "intern"
    return "unknown"

def _equip_soft(text: str) -> Tuple[List[str], List[str]]:
    t = text.lower()
    eq = sorted({w.upper() if w.isalpha() else w for w in EQUIP_WORDS if w in t})
    sw = sorted({w.upper() if w.isalpha() else w for w in SOFT_WORDS if w in t})
    return eq, sw

def _role(text: str) -> str:
    # простая эвристика
    m = re.search(r"(инженер[\-\s]?геодезист|геодезист|геодез\.|инженер[\-\s]?геодезии)", text.lower())
    return (m.group(1) if m else "Геодезист").capitalize()

def _is_employer(text: str) -> bool:
    tl = text.lower()
    if any(x in tl for x in SEEKER_MARKERS): return False
    if any(x in tl for x in EMPLOYER_MARKERS): return True
    # по умолчанию считаем, что это вакансия
    return True

def _city_country(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # лёгкая эвристика; детальную геокодировку добавим позже
    m = re.search(r"(?:г\.|город|в\s+городе)\s*([A-ЯЁA-Za-z\-\s]+)", text)
    city = m.group(1).strip() if m else None
    return city, None, None

def _dedup_hash(text: str) -> str:
    clean = re.sub(r"\s+", " ", text).strip().lower()
    return hashlib.md5(clean.encode("utf-8")).hexdigest()

def parse_job(text: str) -> Dict[str, Any]:
    salary_min, salary_max, currency, period, salary_raw = _parse_salary(text)
    contacts = _contacts(text)
    schedule = _schedule(text)
    employment = _employment(text)
    eq, sw = _equip_soft(text)
    role = _role(text)
    is_emp = _is_employer(text)
    city, region, country = _city_country(text)
    return {
        "role": role,
        "is_employer": is_emp,
        "description": text,
        "salary_min": salary_min, "salary_max": salary_max,
        "salary_currency": currency, "salary_period": period,
        "contact_phone": contacts["phone"],
        "contact_email": contacts["email"],
        "contact_telegram": contacts["telegram"],
        "schedule_type": schedule,
        "employment_type": employment,
        "equipment": eq, "software": sw,
        "city": city, "region": region, "country": country,
        "dedup_hash": _dedup_hash(text),
        "confidence": 0.5,  # заглушка, позже дадим нормальную оценку
    }

# ---------- DB pipeline ----------
def get_conn():
    return psycopg.connect(DB_URL, row_factory=dict_row)

def fetch_unprocessed(cur, limit=200) -> List[Dict[str, Any]]:
    cur.execute("""
        select ri.id as raw_id, ri.text_raw, ri.published_at, ri.source_id
        from public.raw_items ri
        left join public.jobs j on j.raw_item_id = ri.id
        where j.id is null
          and coalesce(ri.text_raw, '') <> ''
        order by ri.id asc
        limit %s
    """, (limit,))
    return cur.fetchall()

def insert_job(cur, row: Dict[str, Any]):
    data = parse_job(row["text_raw"])
    cur.execute("""
        insert into public.jobs
            (source_id, raw_item_id, role, employer_name, is_employer, description,
             contact_telegram, contact_phone, contact_email,
             salary_min, salary_max, salary_currency, salary_period,
             employment_type, schedule_type,
             city, region, country,
             equipment, software,
             experience, language,
             dedup_hash, confidence, posted_at)
        values
            (%s,%s,%s,%s,%s,%s,
             %s,%s,%s,
             %s,%s,%s,%s,
             %s,%s,
             %s,%s,%s,
             %s,%s,
             %s,%s,
             %s,%s,%s)
        on conflict do nothing
        returning id
    """, (
        row["source_id"], row["raw_id"],
        data["role"], None, data["is_employer"], data["description"],
        data["contact_telegram"], data["contact_phone"], data["contact_email"],
        data["salary_min"], data["salary_max"], data["salary_currency"], data["salary_period"],
        data["employment_type"], data["schedule_type"],
        data["city"], data["region"], data["country"],
        data["equipment"], data["software"],
        None, None,
        data["dedup_hash"], data["confidence"], row["published_at"]
    ))
    r = cur.fetchone()
    return r["id"] if r else None

def main():
    log("🚀 Extract pass started")
    inserted = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            rows = fetch_unprocessed(cur, limit=500)
            if not rows:
                log("😴 No unprocessed raw_items"); return
            for r in rows:
                try:
                    job_id = insert_job(cur, r)
                    if job_id:
                        inserted += 1
                except Exception as e:
                    log(f"💥 failed on raw_id={r['raw_id']}: {e}")
            conn.commit()
    log(f"✅ Extracted {inserted} job(s)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"💥 ERROR: {e}")
        raise
