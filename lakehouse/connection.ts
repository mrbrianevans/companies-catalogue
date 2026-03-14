import { randomUUIDv7, S3Client } from "bun";
import { DuckDBConnection, DuckDBInstance } from "@duckdb/node-api";
import { tmpdir } from "node:os";

const lakeBucket = new S3Client({ bucket: process.env.LAKE_BUCKET });

export async function setupLakehouseConnection(existingPath?: string) {
  const tmpDbFilepath = existingPath ?? tmpdir() + `/${randomUUIDv7()}_catalogue.ducklake`;
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
  if (await tempDbFile.exists()) {
    console.log("using existing local lakehouse catalogue", tempDbFile.name);
  } else if (await catalogueDbFile.exists()) {
    console.time("downloaded lakehouse catalogue to " + tempDbFile.name);
    await tempDbFile.write(await catalogueDbFile.bytes());
    console.timeEnd("downloaded lakehouse catalogue to " + tempDbFile.name);
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
  deleteLocal = true,
}: {
  connection: DuckDBConnection;
  tempDbFile: Bun.BunFile;
  remoteCataloguePath: string;
  deleteLocal?: boolean;
}) {
  console.log("[saveAndCloseLakehouse] Starting save and close process");
  console.log("[saveAndCloseLakehouse] tempDbFile:", tempDbFile.name);
  console.log("[saveAndCloseLakehouse] remoteCataloguePath:", remoteCataloguePath);
  console.log("[saveAndCloseLakehouse] deleteLocal:", deleteLocal);

  console.log("[saveAndCloseLakehouse] Attaching :memory: as memory_db");
  await connection.run(`
    ATTACH ':memory:' AS memory_db;
    USE memory_db;
    `);
  console.log("[saveAndCloseLakehouse] Successfully attached and switched to memory_db");

  console.log("[saveAndCloseLakehouse] Detaching lakehouse");
  await connection.run("DETACH lakehouse;");
  console.log("[saveAndCloseLakehouse] Successfully detached lakehouse");

  if (deleteLocal) {
    console.log("[saveAndCloseLakehouse] Closing duckdb connection");
    connection.closeSync();
    console.log("[saveAndCloseLakehouse] Closed duckdb connection successfully");
  }

  const finalBytes = await tempDbFile.bytes();
  console.log("[saveAndCloseLakehouse] Bytes copied to memory");

  console.log("[saveAndCloseLakehouse] Starting catalogue upload");
  console.time("uploaded lakehouse catalogue back to" + remoteCataloguePath);
  await lakeBucket.write(remoteCataloguePath, finalBytes);
  console.timeEnd("uploaded lakehouse catalogue back to" + remoteCataloguePath);
  console.log("[saveAndCloseLakehouse] Successfully uploaded catalogue");

  if (deleteLocal) {
    console.log("[saveAndCloseLakehouse] Deleting local tempDbFile");
    await tempDbFile.delete();
    console.log("[saveAndCloseLakehouse] Successfully deleted local tempDbFile");
  } else {
    console.log("[saveAndCloseLakehouse] Skipping local file deletion (deleteLocal=false)");
  }

  console.log("[saveAndCloseLakehouse] Completed save and close process");
}
