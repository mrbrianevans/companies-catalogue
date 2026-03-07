// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)
// Ducklake catalog is frozen on S3.

import { executeSql, streams } from "./utils.js";
import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";

import lakehouseSnapshotSql from "./lakehouse_snapshot.sql" with { type: "text" };
import lakehouseEventsSql from "./lakehouse_events.sql" with { type: "text" };
import lakehouseSetupSql from "./lakehouse_setup.sql" with { type: "text" };
import { DuckDBListValue } from "@duckdb/node-api";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }
  console.log("Loading", streamPath, "into lakehouse");
  const { connection, tempDbFile, remoteCataloguePath } = await setupLakehouseConnection();
  await connection.run(`CREATE SCHEMA IF NOT EXISTS lakehouse.${getSchema(streamPath)};`);
  await connection.run(`USE lakehouse.${getSchema(streamPath)};`);
  await connection.run(`SET VARIABLE SINK_BUCKET = '${process.env.SINK_BUCKET}';`);
  await connection.run(`SET VARIABLE streamPath = '${streamPath}';`);

  console.time("setup lakehouse");
  await executeSql(connection, lakehouseSetupSql);
  console.timeEnd("setup lakehouse");

  while (true) {
    const filesRemaining = await connection.runAndReadAll(`SELECT list(file) as files FROM
    (FROM glob('s3://'||getvariable('SINK_BUCKET')||'/'||getvariable('streamPath')||'/*.json.gz')
    WHERE file NOT IN (SELECT file FROM cc_metadata.loaded_files))`);
    const files = filesRemaining.getRowObjects()[0].files as DuckDBListValue;
    if (!files?.items?.length) break;
    console.log("Files remaining", files.items);
    console.time("load events");
    await executeSql(connection, lakehouseEventsSql);
    console.timeEnd("load events");
  }

  console.log("Merging any unmerged events into the snapshot");
  console.time("merge snapshot");
  await connection.run(lakehouseSnapshotSql);
  console.timeEnd("merge snapshot");

  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath });
}

await main(process.argv[2]);
