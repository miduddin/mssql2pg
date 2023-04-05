# mssql2pg

Continuously replicates data from SQL Server to PostgreSQL with minimal table locking.

Limitations:

- Can only replicate tables with primary keys.
- Does not copy schema. This also means that schema updates on source after the data replication is started must be manually applied on destination too.

What this program does in general:

1. Existing data is copied using `SELECT ... WITH (NOLOCK)` on SQL Server's side, and `COPY ...` on Postgres' side.
2. Table updates are continuously replicated using [Change Tracking](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server) feature in SQL Server.
