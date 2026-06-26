import { type FileConfig, uploadLocalFiles } from "../lakehouse/utils.ts";
import { tmpdir } from "node:os";
import { randomUUIDv7 } from "bun";
import { mkdir } from "fs/promises";
import type { DuckDBConnection } from "@duckdb/node-api";

const snapshotBucket = process.env.SNAPSHOT_BUCKET;

const entities = [
  "charity",
  "charity_annual_return_history",
  "charity_annual_return_parta",
  "charity_annual_return_partb",
  "charity_area_of_operation",
  "charity_classification",
  "charity_event_history",
  "charity_governing_document",
  "charity_other_names",
  "charity_other_regulators",
  "charity_policy",
  "charity_published_report",
  "charity_trustee",
];

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
    sample: false,
    single: true,
    contentType: "application/zstd",
  },
  {
    format: "json",
    compression: "zstd",
    extension: ".json.zst",
    description: "JSON (zstd)",
    split: false,
    sample: false,
    single: true,
    contentType: "application/zstd",
  },
];

export async function snapshotCharityData(
  connection: DuckDBConnection,
  productionDatetime: string,
) {
  if (!snapshotBucket) {
    throw new Error("SNAPSHOT_BUCKET environment variable is required");
  }
  const productionDate = productionDatetime.split("T")[0];

  const outputDir = tmpdir() + "/companies-catalogue/" + randomUUIDv7() + "/charity";
  console.log("Exporting to local directory", outputDir);
  await mkdir(outputDir, { recursive: true });

  const outputFiles: any[] = [];
  const tableMetadata: Record<string, any> = {};

  for (const entity of entities) {
    console.time(`create local copy of ${entity}`);
    await connection.run(`
      CREATE OR REPLACE TABLE local.charity.${entity} AS 
      SELECT * FROM lakehouse.charity.${entity};
    `);
    console.timeEnd(`create local copy of ${entity}`);

    const entityDownloads: any[] = [];
    for (const file of fileTypes) {
      const outFileName = `${entity}_${productionDate}${file.extension}`;
      console.log("Exporting", entity, file.description);
      console.time(`export ${entity} ${file.extension}`);
      const filesRes = await connection.runAndReadAll(`
          COPY (FROM local.charity.${entity}) 
          TO '${outputDir}/${outFileName}'
          (FORMAT ${file.format}, COMPRESSION ${file.compression}, RETURN_FILES true);
          `);
      console.timeEnd(`export ${entity} ${file.extension}`);

      const outputs = await uploadLocalFiles(
        filesRes,
        file,
        `charity/`,
        { table: entity },
        snapshotBucket,
      );
      entityDownloads.push(...outputs);
      outputFiles.push(...outputs);
    }

    //TODO: query the extract date from the table to include in manifest metadata
    const metadataRes = await connection.runAndReadAll(`
      SELECT 
          COUNT(*)::FLOAT as recordCount
      FROM local.charity.${entity};
  `);
    const metadataRow = metadataRes.getRowObjects()[0];
    console.log(`Metadata ${entity}`, metadataRow);

    tableMetadata[entity] = {
      ...metadataRow,
      downloads: entityDownloads,
    };

    await connection.run(`DROP TABLE IF EXISTS local.charity.${entity};`);
    console.log(`Dropped local ${entity} to free space`);
  }

  console.log("Exported files", JSON.stringify(outputFiles));

  const manifest = {
    snapshotPublishedAt: productionDatetime,
    tables: Object.keys(tableMetadata),
    tableMetadata,
    downloads: outputFiles,
  };
  await Bun.s3.write(`charity-manifest.json`, JSON.stringify(manifest), {
    bucket: snapshotBucket,
    type: "application/json",
  });
  console.log("Manifest uploaded to S3");

  connection.closeSync();
}
