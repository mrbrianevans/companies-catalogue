// Export snapshots from the datalake for convenient download

import { streams } from "./utils.js";
import { setupLakehouseConnection } from "./connection.js";
import { DuckDBListValue } from "@duckdb/node-api";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

const snapshotBucket = process.env.SNAPSHOT_BUCKET;

async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }
  console.log("Exporting", streamPath, "snapshots");
  console.time("setup local catalogue");
  const { connection } = await setupLakehouseConnection();
  console.timeEnd("setup local catalogue");

  await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

  const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`);
  const tables = tablesRes.getRowObjects().map((r) => r.name as string);
  console.log("tables in lakehouse", tables);

  await connection.run(`ATTACH 'temp.db' as local;`);
  await connection.run(`CREATE SCHEMA IF NOT EXISTS local.${getSchema(streamPath)};`);

  console.time("create local snapshot from lakehouse");
  await connection.run(`
    CREATE OR REPLACE TABLE local.${getSchema(streamPath)}.snapshot AS 
    SELECT * FROM lakehouse.${getSchema(streamPath)}.snapshot;
    `);
  console.timeEnd("create local snapshot from lakehouse");

  await connection.run(`SET preserve_insertion_order = false;`);
  const outputFiles = [];
  // export local snapshot to various formats on S3
  const fileTypes = [
    {
      format: "json",
      compression: "none",
      extension: ".json",
      description: "JSON",
      split: false,
      sample: true,
      single: false,
    },
    {
      format: "parquet",
      compression: "snappy",
      extension: ".parquet",
      description: "Parquet",
      split: true,
      sample: true,
      single: false,
    },
    {
      format: "json",
      compression: "gzip",
      extension: ".json.gz",
      description: "JSON (gzip)",
      split: false,
      sample: false,
      single: false,
    },
    {
      format: "json",
      compression: "zstd",
      extension: ".json.zst",
      description: "JSON (zstd)",
      split: false,
      sample: true,
      single: true,
    },
  ];

  //one file
  for (const file of fileTypes.filter((f) => f.single)) {
    console.log("Exporting single file", file.description);
    console.time("export " + file.extension);
    const filesRes = await connection.runAndReadAll(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot) 
        TO 's3://${snapshotBucket}/${streamPath}${file.extension}'
        (FORMAT ${file.format}, COMPRESSION ${file.compression}, RETURN_FILES true);
        `);
    console.timeEnd("export " + file.extension);
    outputFiles.push(
      ...filesRes.getRowObjects().map((fileRow) => ({
        files: (fileRow.Files as DuckDBListValue).items.map((fullName) =>
          (fullName as string).replace(`s3://${snapshotBucket}`, ""),
        ),
        count: Number(fileRow.Count),
        ...file,
      })),
    );
  }

  // split files
  const fileSizeBytes = 128 * 1024 * 1024;
  for (const f of fileTypes.filter((f) => f.split)) {
    console.log("Exporting split files", f.description, fileSizeBytes / 1024 / 1024, "MB max file size");
    //TODO: clean out existing files from path in bucket in case snapshot size decreases and leaves an old partition. (low risk)
    console.time("export split " + f.extension);
    const filesRes = await connection.runAndReadAll(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot)
            TO 's3://${snapshotBucket}/split/${streamPath}'
            (FORMAT ${f.format}, OVERWRITE_OR_IGNORE true, COMPRESSION ${f.compression}, FILE_SIZE_BYTES ${fileSizeBytes}, FILENAME_PATTERN '${streamPath}_part_{i}', RETURN_FILES true);
            `);
    console.timeEnd("export split " + f.extension);
    outputFiles.push(
      ...filesRes.getRowObjects().map((fileRow) => ({
        files: (fileRow.Files as DuckDBListValue).items.map((file) =>
          (file as string).replace(`s3://${snapshotBucket}`, ""),
        ),
        count: Number(fileRow.Count),
        ...f,
      })),
    );
  }

  //sample of 1000 items
  for (const f of fileTypes.filter((f) => f.sample)) {
    console.time("export sample " + f.extension);
    const filesRes = await connection.runAndReadAll(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot USING SAMPLE 1000) 
            TO 's3://${snapshotBucket}/sample/${streamPath}${f.extension}'
            (FORMAT ${f.format}, COMPRESSION ${f.compression}, RETURN_FILES true);
            `);
    console.timeEnd("export sample " + f.extension);
    outputFiles.push(
      ...filesRes.getRowObjects().map((fileRow) => ({
        files: (fileRow.Files as DuckDBListValue).items.map((file) =>
          (file as string).replace(`s3://${snapshotBucket}`, ""),
        ),
        count: Number(fileRow.Count),
        ...f,
      })),
    );
  }

  console.log("Exported files", JSON.stringify(outputFiles));

  const metadataRes = await connection.runAndReadAll(`
    SELECT 
        COUNT(*)::FLOAT as recordCount,
        MIN(event.timepoint)::FLOAT as minTimepoint,
        MAX(event.timepoint)::FLOAT as maxTimepoint,
        MIN(event.published_at) as minPublishedAt,
        MAX(event.published_at) as maxPublishedAt
    FROM local.${getSchema(streamPath)}.snapshot;
`);
  const metadataRow = metadataRes.getRowObjects()[0];
  console.log("Metadata", metadataRow);

  const manifest = {
    streamPath,
    snapshotPublishedAt: new Date().toISOString(),
    ...metadataRow,
    downloads: outputFiles,
  };
  await Bun.s3.write(`${streamPath}-manifest.json`, JSON.stringify(manifest), {
    bucket: snapshotBucket,
    type: "application/json",
  });
  console.log("Manifest uploaded to S3");
  /*
    Other formats to consider in future:
     - CSV via MongoDB (for nested headers)
     - Avro, ORC, Feather, Lance
     - Zip archive compression
     */
  connection.closeSync();
}

await main(process.argv[2]);
