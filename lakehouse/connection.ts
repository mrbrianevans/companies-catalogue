import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";

export async function setupLakehouseConnection() {
  const db = await DuckDBInstance.create(":memory:");
  const connection = await db.connect();
  await connection.run("SET threads = 4;");
  await connection.run("SET memory_limit = '12GB';");
  await connection.run(`
INSTALL httpfs;
LOAD httpfs;
INSTALL ducklake;
INSTALL postgres;
LOAD postgres;

CREATE SECRET s3 (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? "").host}'
);

CREATE SECRET postgres_catalogue (
    TYPE postgres,
    HOST '${process.env.PGHOST}',
    PORT ${process.env.PGPORT},
    DATABASE ${process.env.PGDATABASE},
    USER '${process.env.PGUSER}',
    PASSWORD '${process.env.PGPASSWORD}'
);

CREATE SECRET lakehouse (
    TYPE ducklake,
    METADATA_PATH '',
    DATA_PATH 's3://${process.env.LAKE_BUCKET}/',
    METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_catalogue'},
    METADATA_SCHEMA 'ducklake'
);
`);

  await connection.run(
    `ATTACH 'ducklake:lakehouse' AS lakehouse (CREATE_IF_NOT_EXISTS true, DATA_INLINING_ROW_LIMIT 0);`,
  );
  await connection.run(`ATTACH '' AS catalogue (TYPE postgres, SECRET 'postgres_catalogue');`);
  await connection.run(`USE lakehouse;`);
  return { connection };
}

export async function saveAndCloseLakehouse({ connection }: { connection: DuckDBConnection }) {
  await connection.run(`
    ATTACH ':memory:' AS memory_db;
    USE memory_db;
    `);
  await connection.run("DETACH lakehouse;");
  console.log("Detached lakehouse");
}
