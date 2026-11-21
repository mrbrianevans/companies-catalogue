// ingest using ch-fixedwidth and companies-catalogue s3 bucket.

import { DuckDBInstance } from "@duckdb/node-api";
import { tmpdir } from "node:os";
import { s3Client } from "../eventCapture/utils.ts";
import { rm } from "node:fs/promises";
import { basename } from "node:path";

const baseUrl = "https://s3.companiescatalogue.co.uk";

type LatestMetadata = {
  product_code: string;
  latest_production_date: string;
  latest_paths: { path: string }[];
}[];

const latestMetaRes = await fetch(new URL("/latest.json", baseUrl));
if (!latestMetaRes.ok) {
  console.log("LatestMetaRes not ok", latestMetaRes.status, await latestMetaRes.text());
  throw new Error("Failed to fetch latest.json");
}
const latest = (await latestMetaRes.json()) as LatestMetadata;

const prod216 = latest.find((p) => p.product_code === "prod216");

console.log(prod216);

const prodDate = prod216.latest_production_date;

if (new Date(prodDate).getMonth() === new Date().getMonth()) {
  console.log("This months file is available", prodDate);
} else {
  console.log("This months file is unavailable", prodDate);
  process.exit(0);
}

const companiesRemoteFile = s3Client.file(`fixedwidth/companies_${prodDate}.parquet`);
const personsRemoteFile = s3Client.file(`fixedwidth/persons_${prodDate}.parquet`);
const alreadyExists = (await companiesRemoteFile.exists()) && (await personsRemoteFile.exists());
if (alreadyExists) {
  console.log("This months files already exist on S3");
  process.exit(0);
}

const tmpId = crypto.randomUUID();
const tmp = `${tmpdir()}/ch-fixedwidth/${tmpId}`;
const outdir = `${tmp}/raw`;
const downloadDir = `${tmp}/download`;
for (const { path } of prod216.latest_paths) {
  const url = new URL(path, baseUrl);
  const localFile = `${downloadDir}/${basename(path)}`;
  console.log(url.toString());

  // download to a local file first. Prefer zstd transfer encoding, fetch decompresses transparently.
  console.time(`download ${url}`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download ${url}: ${res.status}`);
  console.log("Content-Encoding", res.headers.get("content-encoding"));
  const bytes = await Bun.write(localFile, res);
  console.timeEnd(`download ${url}`);
  console.log("Downloaded", bytes, "bytes to", localFile);

  console.time(`convert ${url}`);
  const bin = "ch-fixedwidth";
  const proc = Bun.spawn([bin, "-o", outdir, localFile], {
    stdout: "inherit",
    stderr: "inherit",
  });
  const code = await proc.exited;
  console.timeEnd(`convert ${url}`);
  console.log("Exit code for", url.toString(), code);
  // delete the raw file now to avoid accumulating several GB on disk
  await rm(localFile, { force: true });
  if (code !== 0) throw new Error(`ch-fixedwidth exited ${code}`);
}

const db = await DuckDBInstance.create(":memory:");
const connection = await db.connect();

const tmpCompaniesParquet = `${tmp}/companies.parquet`;
const tmpPersonsParquet = `${tmp}/persons.parquet`;

console.time("Write local companies parquet");
await connection.run(
  `COPY (from read_csv('${outdir}/companies_data_*.csv', all_varchar=true, header=true)) TO '${tmpCompaniesParquet}' (COMPRESSION zstd, ROW_GROUP_SIZE 250_000, COMPRESSION_LEVEL 9);`,
);
console.timeEnd("Write local companies parquet");
console.time("Write local persons parquet");
await connection.run(
  `COPY (from read_csv('${outdir}/persons_data_*.csv', all_varchar=true, header=true)) TO '${tmpPersonsParquet}' (COMPRESSION zstd, ROW_GROUP_SIZE 250_000, COMPRESSION_LEVEL 9);`,
);
console.timeEnd("Write local persons parquet");
connection.closeSync();
db.closeSync();

// Upload parquet to S3 to save parquet in the sink bucket:
let parquetFile = Bun.file(tmpCompaniesParquet);
console.log("Bun", Bun.version, "uploading", parquetFile.size, "bytes");
console.time("S3 upload");
await companiesRemoteFile.write(parquetFile);
console.timeEnd("S3 upload");

parquetFile = Bun.file(tmpPersonsParquet);
console.log("Bun", Bun.version, "uploading", parquetFile.size, "bytes");
console.time("S3 upload");
await personsRemoteFile.write(parquetFile);
console.timeEnd("S3 upload");

await rm(tmp, { recursive: true, force: true });
