// create local table of all accounts

// export csv.zst and parquet

// last 2 years to public bucket

// full history to private bucket

import { tmpdir } from "node:os";
import { randomUUIDv7 } from "bun";
import { mkdir } from "fs/promises";
import { uploadLocalFiles } from "./utils.ts";
import type { DuckDBConnection } from "@duckdb/node-api";

const snapshotBucket = process.env.SNAPSHOT_BUCKET;
const privateSnapshotBucket = process.env.PRIVATE_SNAPSHOT_BUCKET;

export async function snapshotXbrl(connection: DuckDBConnection, productionDatetime: string) {
  //TODO: refactor to match the structure of the other snapshots (based on an array of file configs) and include samples/split files.
  console.time("create local snapshot from lakehouse");
  // there can be duplicate data if a daily and monthly file of the same period were ingested
  /* Inspect overlapping files:
    SELECT zip_url, zip_start, zip_end, COUNT(*)
    FROM snapshot
    WHERE zip_start BETWEEN '2026-04-01' AND '2026-04-05'
    GROUP BY zip_url, zip_start, zip_end
    ORDER BY zip_start;
     */
  await connection.run(`
    CREATE  OR REPLACE TABLE local.xbrl.snapshot AS 
    WITH non_daily AS (
    SELECT DISTINCT zip_start, zip_end
    FROM lakehouse.xbrl.xbrl
    WHERE zip_url NOT LIKE '%Accounts_Bulk_Data-%'
)
    SELECT *
    FROM lakehouse.xbrl.xbrl d
    WHERE d.zip_url NOT LIKE '%Accounts_Bulk_Data-%'   -- keep all non-daily
       OR NOT EXISTS (
        SELECT 1
        FROM non_daily m
        WHERE d.zip_start >= m.zip_start
          AND d.zip_start <= m.zip_end     -- daily date falls inside monthly range
    );
    `);
  console.timeEnd("create local snapshot from lakehouse");

  // export from duckdb to local filesystem and then use bun's s3 client to upload to s3
  const outputDir = tmpdir() + "/companies-catalogue/" + randomUUIDv7() + "/xbrl";
  console.log("Exporting to local directory", outputDir);
  await mkdir(outputDir, { recursive: true });
  const outputFiles = [];

  console.log("Exporting last two years");
  // end date should be the last day of the previous month. start date should be 2 years prior on the first day of the month.
  const startDate = new Date();
  startDate.setFullYear(startDate.getFullYear() - 2);
  startDate.setDate(1);
  const endDate = new Date();
  endDate.setDate(1);
  endDate.setDate(endDate.getDate() - 1);
  console.log("start date", startDate.toISOString());
  console.log("end date", endDate.toISOString());
  const datedFileName = `xbrl_${startDate.toISOString().split("T")[0]}--${endDate.toISOString().split("T")[0]}`;

  const outFileName = `${datedFileName}.csv.zst`;
  console.time("export csv.zst");
  const filesRes = await connection.runAndReadAll(`
        COPY (SELECT * EXCLUDE (zip_start, zip_end, csv_name) FROM local.xbrl.snapshot where zip_start >= '${startDate.toISOString()}' and zip_end <= '${endDate.toISOString()}') 
        TO '${outputDir}/${outFileName}'
        (FORMAT csv, COMPRESSION zstd, RETURN_FILES true);
        `);
  console.timeEnd("export csv.zst");
  console.log(filesRes.getRowObjects());
  const outputs = await uploadLocalFiles(
    filesRes,
    {
      compression: "zstd",
      description: "Last two years (CSV Zstd)",
      extension: ".csv.zst",
      format: "csv",
      sample: false,
      single: true,
      split: false,
      contentType: "application/zstd",
    },
    "monthly/",
    { lastTwoYears: true },
    snapshotBucket,
  );
  outputFiles.push(...outputs);

  const parquetOutFileName = `${datedFileName}.parquet`;
  console.time("export parquet");
  const parquetFilesRes = await connection.runAndReadAll(`
        COPY (SELECT * EXCLUDE (zip_start, zip_end, csv_name) FROM local.xbrl.snapshot where zip_start >= '${startDate.toISOString()}' and zip_end <= '${endDate.toISOString()}') 
        TO '${outputDir}/${parquetOutFileName}'
        (FORMAT parquet, RETURN_FILES true);
        `);
  console.timeEnd("export parquet");
  console.log(parquetFilesRes.getRowObjects());
  const parquetOutputs = await uploadLocalFiles(
    parquetFilesRes,
    {
      compression: "snappy",
      description: "Last two years (Parquet)",
      extension: ".parquet",
      format: "parquet",
      sample: false,
      single: true,
      split: false,
    },
    "monthly/",
    { lastTwoYears: true },
    snapshotBucket,
  );
  outputFiles.push(...parquetOutputs);

  const fullHistoryOutFileName = `xbrl_2008-01-01--${endDate.toISOString().split("T")[0]}.parquet`;
  console.time("export full history");
  const fullHistoryFilesRes = await connection.runAndReadAll(`
        COPY (SELECT * EXCLUDE (zip_start, zip_end, csv_name) FROM local.xbrl.snapshot where zip_start >= '2008-01-01' and zip_end <= '${endDate.toISOString()}') 
        TO '${outputDir}/${fullHistoryOutFileName}'
        (FORMAT parquet, RETURN_FILES true);
        `);
  console.timeEnd("export full history");
  console.log(fullHistoryFilesRes.getRowObjects());
  const fullHistoryOutputs = await uploadLocalFiles(
    fullHistoryFilesRes,
    {
      compression: "snappy",
      description: "Full history (Parquet)",
      extension: ".parquet",
      format: "parquet",
      sample: false,
      single: true,
      split: false,
    },
    "monthly/",
    { fullHistory: true },
    privateSnapshotBucket,
  );
  outputFiles.push(...fullHistoryOutputs);

  const manifest = {
    streamPath: "xbrl",
    snapshotPublishedAt: productionDatetime,
    downloads: outputFiles,
  };
  await Bun.s3.write(`xbrl-manifest.json`, JSON.stringify(manifest), {
    bucket: snapshotBucket,
    type: "application/json",
  });
  console.log("Manifest uploaded to S3");
  connection.closeSync();
}
