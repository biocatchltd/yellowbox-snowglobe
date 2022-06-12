from sqlalchemy import text

# this is a script that should be run first thing in any new schema, it includes some adaptations to snowflake
SCHEMA_INITIALIZE_SCRIPT = text("""
create function year(x DATE) returns int as $$select extract(year from $1) $$ language sql stable;
create function month(x DATE) returns int as $$select extract(month from $1) $$ language sql stable;
create function day(x DATE) returns int as $$select extract(day from $1) $$ language sql stable;
CREATE DOMAIN string as TEXT;
""")
