// Export the last 12 complete months of XBRL accounts: full files to the private bucket, samples to the public bucket

import { tmpdir } from "node:os";
import { randomUUIDv7 } from "bun";
import { mkdir } from "fs/promises";
import { type FileConfig, uploadLocalFiles } from "./utils.ts";
import type { DuckDBConnection } from "@duckdb/node-api";

const snapshotBucket = process.env.SNAPSHOT_BUCKET;
const privateSnapshotBucket = process.env.PRIVATE_SNAPSHOT_BUCKET;

const fileTypes: FileConfig[] = [
  {
    format: "parquet",
    compression: "snappy",
    extension: ".parquet",
    description: "Parquet",
    split: false,
    sample: false,
    single: true,
    contentType: "application/vnd.apache.parquet",
  },
  {
    format: "csv",
    compression: "zstd",
    extension: ".csv.zst",
    description: "CSV (zstd)",
    split: false,
    sample: true,
    single: true,
    contentType: "application/zstd",
  },
  {
    format: "json",
    compression: "gzip",
    extension: ".json.gz",
    description: "JSON (gzip)",
    split: false,
    sample: false,
    single: true,
    contentType: "application/gzip",
  },
  {
    format: "json",
    compression: "zstd",
    extension: ".json.zst",
    description: "JSON (zstd)",
    split: false,
    sample: true,
    single: true,
    contentType: "application/zstd",
  },
];

export async function snapshotXbrl(connection: DuckDBConnection, productionDatetime: string) {
  // first day of the month a year ago until the last day of the previous month
  const firstOfMonth = Temporal.Now.plainDateISO("UTC").with({ day: 1 });
  const startDate = firstOfMonth.subtract({ years: 1 }).toString();
  const endDate = firstOfMonth.subtract({ days: 1 }).toString();
  const fileName = `xbrl_${startDate}--${endDate}`;
  console.log("Exporting accounts from", startDate, "to", endDate);

  console.time("create local snapshot from lakehouse");
  await connection.run(`
    CREATE OR REPLACE TABLE local.xbrl.snapshot AS
    SELECT * exclude (archive)
    FROM lakehouse.xbrl.accounts
    WHERE zip_start >= '${startDate}' AND zip_end <= '${endDate}';
  `);
  console.timeEnd("create local snapshot from lakehouse");

  // export from duckdb to local filesystem and then use bun's s3 client to upload to s3
  const outputDir = tmpdir() + "/companies-catalogue/" + randomUUIDv7() + "/xbrl";
  console.log("Exporting to local directory", outputDir);
  await mkdir(`${outputDir}/sample`, { recursive: true });

  const exports = [
    {
      kind: "single",
      source: "FROM local.xbrl.snapshot",
      path: (f: FileConfig) => `${outputDir}/${fileName}${f.extension}`,
      prefix: "monthly/",
      bucket: privateSnapshotBucket,
    },
    {
      kind: "sample",
      source: "FROM local.xbrl.snapshot USING SAMPLE 100",
      path: (f: FileConfig) => `${outputDir}/sample/xbrl${f.extension}`,
      prefix: "sample/",
      bucket: snapshotBucket,
    },
  ] as const;

  const outputFiles = [];
  for (const { kind, source, path, prefix, bucket } of exports) {
    for (const file of fileTypes.filter((f) => f[kind])) {
      const label = `export ${kind} ${file.extension}`;
      console.time(label);
      const filesRes = await connection.runAndReadAll(`
        COPY (${source})
        TO '${path(file)}'
        (FORMAT ${file.format}, COMPRESSION ${file.compression}, RETURN_FILES true);
      `);
      console.timeEnd(label);
      outputFiles.push(
        ...(await uploadLocalFiles(filesRes, file, prefix, { [kind]: true }, bucket)),
      );
    }
  }
  console.log("Exported files", JSON.stringify(outputFiles));

  const manifest = {
    streamPath: "xbrl",
    snapshotPublishedAt: productionDatetime,
    startDate,
    endDate,
    downloads: outputFiles,
  };
  await Bun.s3.write(`xbrl-manifest.json`, JSON.stringify(manifest), {
    bucket: snapshotBucket,
    type: "application/json",
  });
  console.log("Manifest uploaded to S3");
  connection.closeSync();
}
