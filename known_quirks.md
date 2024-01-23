* due to the way postgresql handles databases, "use database" must be used before any schema or table is referred to.
  * object can be referred to by their database, but they must already be connected to that database with "use database".
* `SELECT`
  * the names of columns might differ from snowflake implementation (unless explicitly specified with `as`).
* `SHOW SCHEMAS;`/`SHOW TABLES;`
  * only returns the objects of the current database, and only supports the `LIMIT` option
  * snowglobe attempts to return a structure a similar to snowflake as possible, but many fields will be null.
* `DESCRIBE TABLE` 
  * is only supported without any `TYPE` parameter
* `CREATE SCHEMA`
  * Only the "If not exists" option is supported
* `CREATE TABLE`
  * Only the "If not exists" option is supported
* `CREATE VIEW`
  * Only the "OR REPLACE" option is supported
* `CREATE DATABASE`
  * This command is ignored entirely, snowglobe assumes that any database a session switches to already exists (and
  creates it on the fly if needed)
* `flatten`
  * the resulting table will only have the `values` column
* async queries
  * snowglobe always handles queries synchronously. It also stores async results to be retieved later as 
    though they were async. As such, snowglobe queries will never be in a "pending" state.
  * all stored async results are cleared when retrieved, this means that each async result can only
    be retrieved once.
* from all the timestamp types in snowflake, only TIMESTAMP_NTZ is currently supported.
* `Json Queries`
  * Supports querying json data using {Column_Name}.{Json_Key}::number and {Column_Name}.{Json_Key}::string syntax.
  * Nested json lookups are not supported.