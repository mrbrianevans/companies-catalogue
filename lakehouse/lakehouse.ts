// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)

import { executeSql, streams } from "./utils.js";
import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";

import lakehouseEventsSql from "./lakehouse_events.sql" with { type: "text" };
import lakehouseSetupSql from "./lakehouse_setup.sql" with { type: "text" };
import lakehouseXbrlSql from "./lakehouse_xbrl.sql" with { type: "text" };
import lakehouseSetupXbrlSql from "./lakehouse_setup_xbrl.sql" with { type: "text" };
import { DuckDBListValue } from "@duckdb/node-api";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

async function main(streamPath: string) {
  if (!streams.includes(streamPath) && streamPath !== "xbrl") {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }
  console.log("Loading", streamPath, "into lakehouse");
  const { connection } = await setupLakehouseConnection();
  await connection.run(`CREATE SCHEMA IF NOT EXISTS lakehouse.${getSchema(streamPath)};`);
  await connection.run(`USE lakehouse.${getSchema(streamPath)};`);
  await connection.run(`SET VARIABLE SINK_BUCKET = '${process.env.SINK_BUCKET}';`);
  await connection.run(`SET VARIABLE streamPath = '${streamPath}';`);

  const setupScript = streamPath === "xbrl" ? lakehouseSetupXbrlSql : lakehouseSetupSql;
  const fileExtension = streamPath === "xbrl" ? ".csv" : ".json.gz";
  const lakehouseSql = streamPath === "xbrl" ? lakehouseXbrlSql : lakehouseEventsSql;

  console.time("setup lakehouse");
  await executeSql(connection, setupScript);
  console.timeEnd("setup lakehouse");

  while (true) {
    console.time("check for unloaded files");
    const filesRemaining = await connection.runAndReadAll(`SELECT list(file) as files FROM
    (FROM glob('s3://'||getvariable('SINK_BUCKET')||'/'||getvariable('streamPath')||'/*${fileExtension}')
    WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files))`);
    const files = filesRemaining.getRowObjects()[0].files as DuckDBListValue;
    console.timeEnd("check for unloaded files");
    console.log("Files remaining", files?.items?.length);
    if (!files?.items?.length) break;
    console.time("load events");
    await executeSql(connection, lakehouseSql);
    console.timeEnd("load events");
  }

  await saveAndCloseLakehouse({ connection });
}

await main(process.argv[2]);
