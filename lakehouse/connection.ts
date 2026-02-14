import { randomUUIDv7, S3Client } from "bun";
import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { tmpdir } from "node:os";

const lakeBucket = new S3Client({ bucket: process.env.LAKE_BUCKET });

export async function setupLakehouseConnection() {
  const tmpDbFilepath = tmpdir() + `/${randomUUIDv7()}_catalogue.ducklake`;
  const tempDbFile = Bun.file(tmpDbFilepath);
  const db = await DuckDBInstance.create(":memory:");
  const connection = await db.connect();
  await connection.run("SET threads = 1;");
  await connection.run(`
INSTALL httpfs;
LOAD httpfs;
INSTALL ducklake;

CREATE SECRET s3 (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? "").host}'
);

CREATE SECRET lakehouse (
    TYPE ducklake,
    METADATA_PATH '${tempDbFile.name}',
    DATA_PATH 's3://${process.env.LAKE_BUCKET}/'
);
`);

  const remoteCataloguePath = "catalogue.ducklake";
  const catalogueDbFile = lakeBucket.file(remoteCataloguePath);
  if (await catalogueDbFile.exists()) {
    await tempDbFile.write(await catalogueDbFile.bytes());
    console.log("downloaded lakehouse catalogue to", tempDbFile.name);
  } else {
    console.log("no lakehouse catalogue found, creating one.");
  }

  await connection.run(`ATTACH 'ducklake:lakehouse' AS lakehouse (CREATE_IF_NOT_EXISTS true);`);
  await connection.run(`USE lakehouse;`);
  return { connection, tempDbFile, remoteCataloguePath };
}

export async function saveAndCloseLakehouse({
  connection,
  tempDbFile,
  remoteCataloguePath,
  detach = true,
}: {
  connection: DuckDBConnection;
  tempDbFile: Bun.BunFile;
  remoteCataloguePath: string;
  detach?: boolean;
}) {
  if (detach) {
    await connection.run(`
    ATTACH ':memory:' AS memory_db;
    USE memory_db;
    `);
    await connection.run("DETACH lakehouse;");
  }

  await lakeBucket.write(remoteCataloguePath, tempDbFile);
  console.log("uploaded lakehouse catalogue back to", remoteCataloguePath);

  if (detach) await tempDbFile.delete();
}
