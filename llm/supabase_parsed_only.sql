-- Create only the parsed table; assume your raw table already exists (e.g., public.raw_items).
create table if not exists public.parsed_jobs (
  raw_id bigint primary key,
  is_candidate boolean not null,
  is_employer boolean not null,
  position_title text,
  city text,
  country text,
  work_format text,
  experience_years numeric,
  salary_from numeric,
  salary_to numeric,
  salary_currency text,
  salary_period text,
  skills text[] default array[]::text[],
  equipment text[] default array[]::text[],
  software text[] default array[]::text[],
  education text,
  contacts jsonb default '{}'::jsonb,
  source_text text,
  confidence numeric,

  -- Optional passthrough fields from your raw table (populate via METADATA_COLUMNS)
  source_id text,
  external_id text,
  author text,
  url text,
  published_at timestamptz,
  fetched_at timestamptz,
  attachments jsonb
);

alter table public.parsed_jobs disable row level security;
