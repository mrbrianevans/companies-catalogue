// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)
// Ducklake catalog is frozen on S3.

import { streams } from "./utils.js";
import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";
import lakehouseSnapshotSql from "./lakehouse_snapshot.sql" with { type: "text" };
import lakehouseEventsSql from "./lakehouse_events.sql" with { type: "text" };

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

  console.time("load events");
  await connection.run(lakehouseEventsSql);
  console.timeEnd("load events");

  console.log("Merging any unmerged events into the snapshot");
  console.time("merge snapshot");
  await connection.run(lakehouseSnapshotSql);
  console.timeEnd("merge snapshot");

  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath });
}

await main(process.argv[2]);
