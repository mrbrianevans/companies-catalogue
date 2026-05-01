# Postgres Catalogue for lakehouse

Connection from duckdb:

```sql
CREATE OR REPLACE PERSISTENT SECRET (
    TYPE postgres,
    HOST '...',
    PORT 25060,
    DATABASE 'cc_dev', -- can be overriden later
    USER 'read-write',
    PASSWORD '...'
);
-- assuming you've already got a secret for S3
CREATE or replace persistent SECRET dev_postgres_lake (
    TYPE ducklake,
    METADATA_PATH 'dbname=cc_dev',
    DATA_PATH 's3://dev-companies-stream-lake/',
    METADATA_PARAMETERS MAP {'TYPE': 'postgres'},
    METADATA_SCHEMA 'ducklake'
);
```

Connect to postgres itself:

```sql
attach 'dbname=cc_dev' as postgres (type postgres);
```

Connect to the ducklake in postgres:

```sql
ATTACH 'ducklake:dev_postgres_lake' AS ducklake (automatic_migration false);
```

## Copy catalogue from duckdb to postgres

Start with an in-memory duckdb instance:

```sql
attach 'catalogue.ducklake' as catalogue;
attach 'dbname=cc_dev' as postgres (type postgres);

use catalogue;
export database 'catalogue_ex' (format parquet);
use postgres.ducklake;
import database 'catalogue_ex';
show tables;
```

## Once setup, future connections

```sql
ATTACH 'ducklake:dev_postgres_lake' AS ducklake;
use ducklake.cc_metadata;
```
