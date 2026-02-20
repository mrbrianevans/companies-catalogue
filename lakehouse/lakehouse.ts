// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)
// Ducklake catalog is frozen on S3.

import { streams } from "./utils.js";
import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";
// @ts-ignore
import lakehouseSnapshotSql from "./lakehouse_snapshot.sql" with { type: "text" };
/* TODO:
 *  This process should be converted to a more pure SQL pipeline.
 *  Move the SQL statements to a .sql file which gets read and run.
 *  Replace JS string interpolation by getting bucket name from env vars (getenv('SINK_BUCKET')).
 *  - SET VARIABLE streamPath = '${streamPath}';
 *  - getvariable('streamPath') in usages
 *  Still wrap execution in Bun typescript, but let lakehouse logic all sit in SQL files.
 */

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");
const sinkBucket = process.env.SINK_BUCKET;
async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }
  console.log("Loading", streamPath, "into lakehouse");
  const { connection, tempDbFile, remoteCataloguePath } = await setupLakehouseConnection();
  await connection.run(`CREATE SCHEMA IF NOT EXISTS lakehouse.${getSchema(streamPath)};`);
  await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

  const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`);
  const tables = tablesRes.getRowObjects().map((r) => r.name as string);
  console.log("tables in lakehouse", tables);

  await connection.run(`
        CREATE TABLE IF NOT EXISTS events
        (
            resource_kind VARCHAR,
            resource_id   VARCHAR,
            resource_uri  VARCHAR,
            "data"        JSON,
            "event"       STRUCT(timepoint BIGINT, published_at VARCHAR, "type" VARCHAR)
        );
        CREATE TABLE IF NOT EXISTS snapshot AS FROM events WITH NO DATA;
    `);

  await connection.run(`CREATE SCHEMA IF NOT EXISTS cc_metadata;`);
  await connection.run(`CREATE TABLE IF NOT EXISTS cc_metadata.loaded_files
                          (
                              file VARCHAR
                          )`);
  // find files in S3 that aren't loaded
  console.log("Finding files to load");
  // instead of doing WHERE file not in loaded_files, rather do WHERE file > (select max in loaded_files) to only get newer ones
  const res = await connection.runAndReadAll(`
        SELECT file
        FROM glob('s3://${sinkBucket}/${streamPath}/*.json.gz')
        WHERE file NOT IN (SELECT file FROM cc_metadata.loaded_files)
        ORDER BY file ASC;
    `);

  const allFiles = res.getRowObjects().map((f) => f.file as string);
  const files = allFiles.slice(0, 1); // if this is more than one, you need to apply DISTINCT on events before inserting.

  if (files.length) {
    console.log("Loading", files.length, "of", allFiles.length, "files into lakehouse", files);

    await connection.run("BEGIN TRANSACTION;");
    console.time("load events");
    //TODO: could explicitly filter out error events, although even.timepoint is not null probably handles that.
    await connection.run(`
            INSERT INTO events BY NAME
            (FROM read_json(${JSON.stringify(files)}, columns = {resource_kind : 'VARCHAR',
             resource_id : 'VARCHAR',
             resource_uri : 'VARCHAR',
             data : 'JSON',
             event : 'STRUCT(timepoint BIGINT, published_at VARCHAR, type VARCHAR)'}, auto_detect = false)
                WHERE event.timepoint IS NOT NULL AND event.timepoint > (SELECT COALESCE(MAX(inner_events.event.timepoint), 0) FROM events inner_events)
                );`);
    console.timeEnd("load events");
    console.log("Loaded", files.length, "files into events table");

    await connection.run(`INSERT INTO cc_metadata.loaded_files
        VALUES
        ${files.map((f) => `('${f}')`).join(",")};`);
    console.log("Updated loaded_files table with", files.length, "new files");
    await connection.run("COMMIT;");
  }

  console.log("Merging any unmerged events into the snapshot");
  console.time("merge snapshot");
  await connection.run(lakehouseSnapshotSql);
  console.timeEnd("merge snapshot");

  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath });
}

await main(process.argv[2]);
