// ingest using ch-fixedwidth and companies-catalogue s3 bucket.
// track loaded files in loadedFiles table

import { DuckDBInstance } from "@duckdb/node-api";
import { tmpdir } from "node:os";
import { s3Client } from "../eventCapture/utils.ts";

const baseUrl = "https://s3.companiescatalogue.co.uk";

type LatestMetadata = {
  product_code: string;
  latest_production_date: string;
  latest_paths: { path: string }[];
}[];

const latestMetaRes = await fetch(new URL("/latest.json", baseUrl));
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
const outdir = `${tmpdir()}/ch-fixedwidth/${tmpId}/raw`;
for (const { path } of prod216.latest_paths) {
  const url = new URL(path, baseUrl);
  console.log(url.toString());

  console.time(`convert ${url}`);
  const bin = "ch-fixedwidth";
  const proc = Bun.spawn([bin, "-o", outdir, url.toString()], {
    stdout: "inherit",
    stderr: "inherit",
  });
  const code = await proc.exited;
  console.timeEnd(`convert ${url}`);
  console.log("Exit code for", url.toString(), code);
  // if (code !== 0) throw new Error(`ch-fixedwidth exited ${code}`);
}

const db = await DuckDBInstance.create(":memory:");
const connection = await db.connect();

const tmpCompaniesParquet = `${tmpdir()}/ch-fixedwidth/${tmpId}/companies.parquet`;
const tmpPersonsParquet = `${tmpdir()}/ch-fixedwidth/${tmpId}/persons.parquet`;

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
await Bun.file(tmpCompaniesParquet).delete();

parquetFile = Bun.file(tmpPersonsParquet);
console.log("Bun", Bun.version, "uploading", parquetFile.size, "bytes");
console.time("S3 upload");
await personsRemoteFile.write(parquetFile);
console.timeEnd("S3 upload");
await Bun.file(tmpPersonsParquet).delete();

await Bun.file(outdir).delete();
