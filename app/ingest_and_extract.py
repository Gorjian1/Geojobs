import os
import sys
from datetime import datetime, timezone
from dateutil import tz
import psycopg
from psycopg.rows import dict_row

# ---------- helpers ----------
def log(msg: str):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_utc} UTC] {msg}", flush=True)

def get_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        log(f"❌ ENV {name} is missing")
        sys.exit(1)
    return val

# ---------- main ----------
def main():
    log("✅ Ingest job started")
    db_url = get_env("DATABASE_URL")

    # Подключение к БД (SSL обязателен для Supabase; обычно уже есть в URL ?sslmode=require)
    # autocommit выключен: явный commit/rollback
    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # 1) Убедимся, что источник существует (простая upsert-логика)
            src_kind = "telegram"
            src_name = "Тестовый канал"
            src_url  = "https://t.me/example1"

            cur.execute("""
                select id from public.sources
                where kind = %s and coalesce(url,'') = %s and coalesce(name,'') = %s
                limit 1
            """, (src_kind, src_url, src_name))
            row = cur.fetchone()
            if row:
                source_id = row["id"]
                log(f"ℹ️  Source exists: id={source_id}")
            else:
                cur.execute("""
                    insert into public.sources (kind, name, url)
                    values (%s, %s, %s)
                    returning id
                """, (src_kind, src_name, src_url))
                source_id = cur.fetchone()["id"]
                log(f"➕ Source created: id={source_id}")

            # 2) Вставим тестовый raw_item.
            #    У raw_items стоит unique (source_id, external_id) — используем стабильный external_id,
            #    чтобы повторные запуски не плодили дубликаты.
            #    В реальном сборщике external_id = message_id из Telegram.
            external_id = "test-message-0001"
            published_at = datetime.now(timezone.utc)
            author = "test_user"
            url = "https://t.me/example1/123"
            text_raw = (
                "ВАКАНСИЯ: Инженер-геодезист. Вахта 30/15, оплата 200 000–250 000 ₽/мес. "
                "Требования: опыт работы с тахеометром и GNSS (Trimble/Leica), AutoCAD/Civil 3D. "
                "Контакты: @hr_example, +7 (999) 123-45-67, hr@example.com. Локация: Тюмень/ХМАО."
            )

            cur.execute("""
                insert into public.raw_items
                    (source_id, external_id, published_at, author, url, text_raw, attachments)
                values
                    (%s, %s, %s, %s, %s, %s, %s)
                on conflict (source_id, external_id) do nothing
                returning id
            """, (source_id, external_id, published_at, author, url, text_raw, None))

            inserted = cur.fetchone()
            if inserted:
                raw_id = inserted["id"]
                log(f"🧾 raw_items inserted: id={raw_id}")
            else:
                # запись уже есть — узнаем её id (полезно для последующих шагов)
                cur.execute("""
                    select id from public.raw_items
                    where source_id=%s and external_id=%s
                """, (source_id, external_id))
                raw_id = cur.fetchone()["id"]
                log(f"↺ raw_items already exists: id={raw_id}")

            # 3) (Опционально) Пример вставки в jobs "черновика" вакансии.
            #    В реальном пайплайне это делает экстрактор (LLM+правила).
            role = "Инженер-геодезист"
            description = text_raw
            posted_at = published_at

            cur.execute("""
                insert into public.jobs
                    (source_id, raw_item_id, role, employer_name, is_employer,
                     description, salary_min, salary_max, salary_currency, salary_period,
                     employment_type, schedule_type, city, region, country,
                     posted_at)
                values
                    (%s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s,
                     %s)
                returning id
            """, (
                source_id, raw_id, role, "ООО «ГеоТест»", True,
                description, 200000, 250000, "RUB", "month",
                "full", "вахта", "Тюмень", "Тюменская область", "RU",
                posted_at
            ))
            job_id = cur.fetchone()["id"]
            log(f"💾 jobs inserted: id={job_id} (fts-триггер заполнит индекс автоматически)")

            # 4) Пример простого отчёта
            cur.execute("select count(*) as c from public.raw_items")
            total_raw = cur.fetchone()["c"]
            cur.execute("select count(*) as c from public.jobs")
            total_jobs = cur.fetchone()["c"]
            log(f"📊 Totals — raw_items: {total_raw}, jobs: {total_jobs}")

        # фиксируем все изменения
        conn.commit()

    # Для наглядности время в твоём поясе (Европа/Амстердам)
    ams = datetime.now(timezone.utc).astimezone(tz.gettz("Europe/Amsterdam"))
    log(f"🕒 Finished. Local time (Amsterdam): {ams.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"💥 ERROR: {e}")
        # чтобы ошибка подсветилась «красным» в Actions
        raise
