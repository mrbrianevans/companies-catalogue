// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)
// Ducklake catalog is frozen on S3.

import { streams } from "./utils.js";
import { saveAndCloseLakehouse, setupLakehouseConnection } from "../historicalEvents/connection.js";

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
        FROM glob('s3://companies-stream-sink/${streamPath}/*.json.gz')
        WHERE file NOT IN (SELECT file FROM cc_metadata.loaded_files)
        ORDER BY file ASC;
    `);

  const allFiles = res.getRowObjects().map((f) => f.file as string);
  const files = allFiles.slice(0, 1); // if this is more than one, you need to apply DISTINCT on events before inserting.

  await connection.run("BEGIN TRANSACTION;");
  if (files.length) {
    console.log("Loading", files.length, "of", allFiles.length, "files into lakehouse", files);

    console.time("load events");
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
  }

  console.log("Merging any unmerged events into the snapshot");
  console.time("merge snapshot");
  const newEventsSql = `
        SELECT *
        FROM events
        WHERE event.timepoint > (SELECT COALESCE(MAX(event.timepoint), 0) FROM snapshot)
        ORDER BY event.timepoint ASC
        -- at most 1 million events at a time
        LIMIT 1000000
    `;
  await connection.run(`
        WITH new_events AS (${newEventsSql}),
            latest AS (SELECT resource_uri, MAX(event.timepoint) as max_timepoint
                       FROM new_events
                       GROUP BY resource_uri),
            deduped AS (SELECT e.*
                        FROM new_events e
                                 INNER JOIN latest ON e.event.timepoint = latest.max_timepoint)
            MERGE INTO snapshot
        USING (FROM deduped)
            USING
            (resource_uri)
            WHEN NOT MATCHED THEN
        INSERT BY NAME
            WHEN MATCHED THEN
        UPDATE;
    `);
  console.timeEnd("merge snapshot");

  await connection.run("COMMIT;");

  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath });
}

await main(process.argv[2]);
