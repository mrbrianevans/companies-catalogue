// Export snapshots from the datalake for convenient download

import { streams } from "./utils.js";
import { setupLakehouseConnection } from "./connection.js";
import { DuckDBListValue, DuckDBResultReader } from "@duckdb/node-api";
import { tmpdir } from "node:os";
import { randomUUIDv7 } from "bun";
import { basename } from "node:path";
import { mkdir } from "fs/promises";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

const snapshotBucket = process.env.SNAPSHOT_BUCKET;
const privateSnapshotBucket = process.env.PRIVATE_SNAPSHOT_BUCKET;
type FileConfig = {
  format: string;
  compression: string;
  extension: string;
  description: string;
  split: boolean;
  sample: boolean;
  single: boolean;
  contentType?: string;
  contentEncoding?: string;
};
async function uploadLocalFiles(
  filesRes: DuckDBResultReader,
  file: FileConfig,
  prefix: string,
  metadata: Record<string, any> = {},
  bucket: string,
) {
  const outputs = [];
  for (const outputFileBatch of filesRes.getRowObjects()) {
    const actualOutputNames = [];
    for (const outputFile of (outputFileBatch.Files as DuckDBListValue).items as string[]) {
      const localFile = Bun.file(outputFile);
      const actualFilename = basename(outputFile);
      console.log("Uploading", actualFilename, "to S3");
      await Bun.s3.write(prefix + actualFilename, localFile, {
        bucket,
        type: file.contentType,
        contentEncoding: file.contentEncoding,
        contentDisposition: `attachment; filename="${actualFilename}"`,
      });
      actualOutputNames.push(prefix + actualFilename);
    }
    outputs.push({
      files: actualOutputNames,
      count: Number(outputFileBatch.Count),
      format: file.format,
      extension: file.extension,
      compression: file.compression,
      description: file.description,
      ...metadata,
    });
  }
  return outputs;
}

async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }
  const productionDatetime = new Date().toISOString();
  const productionDate = productionDatetime.split("T")[0];
  console.log("Exporting", streamPath, "snapshots on date", productionDate);
  console.time("setup local catalogue");
  const { connection } = await setupLakehouseConnection(
    "C:\\Users\\bme\\AppData\\Local\\Temp/019d9721-4e00-7000-b5a6-294dd1be7c20_catalogue.ducklake",
  );
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
  const fileTypes: FileConfig[] = [
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
      split: false,
      sample: false,
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
      contentType: "application/json",
      contentEncoding: "zstd",
    },
  ];
  // export from duckdb to local filesystem and then use bun's s3 client to upload to s3
  const outputDir = tmpdir() + "/companies-catalogue/" + randomUUIDv7() + "/" + streamPath;
  console.log("Exporting to local directory", outputDir);
  await mkdir(outputDir, { recursive: true });
  //one file
  for (const file of fileTypes.filter((f) => f.single)) {
    const outFileName = `${streamPath}_${productionDate}${file.extension}`;
    console.log("Exporting single file", file.description);
    console.time("export " + file.extension);
    const filesRes = await connection.runAndReadAll(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot) 
        TO '${outputDir}/${outFileName}'
        (FORMAT ${file.format}, COMPRESSION ${file.compression}, RETURN_FILES true);
        `);
    console.timeEnd("export " + file.extension);

    const outputs = await uploadLocalFiles(
      filesRes,
      file,
      "",
      { single: true },
      privateSnapshotBucket,
    );
    outputFiles.push(...outputs);
  }

  // split files
  const fileSizeBytes = 128 * 1024 * 1024;
  for (const f of fileTypes.filter((fileType) => fileType.split)) {
    const outFilePrefix = `${streamPath}_${productionDate}`;
    console.log(
      "Exporting split files",
      f.description,
      fileSizeBytes / 1024 / 1024,
      "MB max file size",
    );
    console.time("export split " + f.extension);
    const filesRes = await connection.runAndReadAll(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot)
            TO '${outputDir}/${outFilePrefix}'
            (FORMAT ${f.format}, OVERWRITE_OR_IGNORE true, COMPRESSION ${f.compression}, FILE_SIZE_BYTES ${fileSizeBytes}, FILENAME_PATTERN '${streamPath}_part_{i}', RETURN_FILES true);
            `);
    console.timeEnd("export split " + f.extension);

    const outputs = await uploadLocalFiles(
      filesRes,
      f,
      "split/",
      { split: true },
      privateSnapshotBucket,
    );
    outputFiles.push(...outputs);
  }

  //sample of 1000 items in public bucket
  for (const f of fileTypes.filter((fileType) => fileType.sample)) {
    console.time("export sample " + f.extension);
    const filesRes = await connection.runAndReadAll(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot USING SAMPLE 1000) 
            TO 's3://${snapshotBucket}/sample/${streamPath}${f.extension}'
            (FORMAT ${f.format}, COMPRESSION ${f.compression}, RETURN_FILES true);
            `);
    console.timeEnd("export sample " + f.extension);

    const outputs = await uploadLocalFiles(filesRes, f, "split/", { sample: true }, snapshotBucket);
    outputFiles.push(...outputs);
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
    snapshotPublishedAt: productionDatetime,
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
