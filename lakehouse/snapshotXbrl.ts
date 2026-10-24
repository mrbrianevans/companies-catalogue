// Export XBRL accounts for the last 12 complete months and the full history since 2010: full files to the private bucket, samples to the public bucket

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
    format: "csv",
    compression: "none",
    extension: ".csv",
    description: "CSV",
    split: false,
    sample: true,
    single: false,
    contentType: "text/csv",
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
  // periods end on the last day of the previous month
  const firstOfMonth = Temporal.Now.plainDateISO("UTC").with({ day: 1 });
  const fullHistoryStartDate = "2010-01-01";
  const endDate = firstOfMonth.subtract({ days: 1 }).toString();
  const periods = [
    { period: "lastYear", startDate: firstOfMonth.subtract({ years: 1 }).toString() },
    { period: "fullHistory", startDate: fullHistoryStartDate },
  ];

  console.time("create local snapshot from lakehouse");
  await connection.run(`
    CREATE OR REPLACE TABLE local.xbrl.snapshot AS
    SELECT * exclude (archive)
    FROM lakehouse.xbrl.accounts
    WHERE zip_start >= '${fullHistoryStartDate}' AND zip_end <= '${endDate}';
  `);
  console.timeEnd("create local snapshot from lakehouse");

  // export from duckdb to local filesystem and then use bun's s3 client to upload to s3
  const outputDir = tmpdir() + "/companies-catalogue/" + randomUUIDv7() + "/xbrl";
  console.log("Exporting to local directory", outputDir);
  await mkdir(`${outputDir}/sample`, { recursive: true });

  const exports = [
    {
      kind: "single",
      sample: "",
      dir: outputDir,
      prefix: "monthly/",
      bucket: privateSnapshotBucket,
    },
    {
      kind: "sample",
      sample: "USING SAMPLE 100",
      dir: `${outputDir}/sample`,
      prefix: "sample/",
      bucket: snapshotBucket,
    },
  ] as const;

  const outputFiles = [];
  for (const { period, startDate } of periods) {
    const fileName = `xbrl_${startDate}--${endDate}`;
    for (const { kind, sample, dir, prefix, bucket } of exports) {
      for (const file of fileTypes.filter((f) => f[kind])) {
        const label = `export ${period} ${kind} ${file.extension}`;
        console.time(label);
        // sample in an outer query, as USING SAMPLE is applied before WHERE
        const filesRes = await connection.runAndReadAll(`
          COPY (FROM (FROM local.xbrl.snapshot WHERE zip_start >= '${startDate}') ${sample})
          TO '${dir}/${fileName}${file.extension}'
          (FORMAT ${file.format}, COMPRESSION ${file.compression}, RETURN_FILES true);
        `);
        console.timeEnd(label);
        const metadata = { [kind]: true, period, startDate, endDate };
        outputFiles.push(...(await uploadLocalFiles(filesRes, file, prefix, metadata, bucket)));
      }
    }
  }
  console.log("Exported files", JSON.stringify(outputFiles));

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
