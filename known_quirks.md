* due to the way postgresql handles databases, "use database" must be used before any schema or table is referred to.
  * object can be referred to by their database, but they must already be connected to that database with "use database".
* `SHOW SCHEMAS;`/`SHOW TABLES;` only returns the objects of the current database, and only supports the `LIMIT` option
  * `SHOW SCHEMAS;`/`SHOW TABLES;` attempts to return a structure a similar to snowflake as possible, but many fields will be null.
* `DESCRIBE TABLE` is only supported without any `TYPE` parameter