from sqlalchemy import text

SCHEMA_INITIALIZE_SCRIPT = text("""
create function year(x DATE) returns int as $$select extract(year from $1) $$ language sql stable;
create function month(x DATE) returns int as $$select extract(month from $1) $$ language sql stable;
create function day(x DATE) returns int as $$select extract(day from $1) $$ language sql stable;
CREATE DOMAIN string as TEXT; 
""" )