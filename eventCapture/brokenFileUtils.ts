import { DuckDBConnection } from "@duckdb/node-api";
import { createGzip, createGunzip } from "node:zlib";
import { createReadStream, createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import split2 from "split2";
import { s3Client } from "./utils.ts";

export type BrokenFileKind = "broken_at_end" | "broken_mid_file";

export interface BrokenFileReport {
  file: string;
  kind: BrokenFileKind;
  lineNumber: number;
  line: string;
  error: string;
}

export function s3KeyFromUri(file: string): string {
  const prefix = `s3://${process.env.SINK_BUCKET}/`;
  if (!file.startsWith(prefix)) throw new Error(`Unexpected file URI: ${file}`);
  return file.slice(prefix.length);
}

export function streamPathFromUri(file: string): string {
  const streamPath = s3KeyFromUri(file).split("/")[0];
  if (!streamPath) throw new Error(`Could not derive stream from URI: ${file}`);
  return streamPath;
}

export function validateS3FileUri(file: string): void {
  const prefix = `s3://${process.env.SINK_BUCKET}/`;
  if (!file) {
    throw new Error(
      `No S3 URI provided. Usage: bun eventCapture/fixBrokenEndFiles.ts s3://${process.env.SINK_BUCKET}/[stream]/[file].json.gz`,
    );
  }
  if (!file.startsWith(prefix)) {
    throw new Error(`Expected S3 URI under ${prefix}, got: ${file}`);
  }
  if (!file.endsWith(".json.gz")) {
    throw new Error(`Expected .json.gz file URI, got: ${file}`);
  }
}

export async function getUnloadedFiles(connection: DuckDBConnection, streamPath: string): Promise<string[]> {
  const sinkBucket = process.env.SINK_BUCKET;
  const res = await connection.runAndReadAll(`
    SELECT file
    FROM glob('s3://${sinkBucket}/${streamPath}/*.json.gz')
    WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files)
    ORDER BY file ASC;
  `);
  return res.getRowObjects().map((row) => row.file as string).toSorted();
}

export async function getAllBucketFiles(connection: DuckDBConnection, streamPath: string): Promise<string[]> {
  const sinkBucket = process.env.SINK_BUCKET;
  const res = await connection.runAndReadAll(`
    SELECT file
    FROM glob('s3://${sinkBucket}/${streamPath}/*.json.gz')
    ORDER BY file ASC;
  `);
  return res.getRowObjects().map((row) => row.file as string).toSorted();
}

export async function isValidJsonFile(connection: DuckDBConnection, file: string): Promise<boolean> {
  try {
    await connection.runAndReadAll(`SELECT COUNT(*) AS count FROM read_json('${file}');`);
    return true;
  } catch {
    return false;
  }
}

export async function downloadFile(file: string, localPath: string) {
  await pipeline(s3Client.file(s3KeyFromUri(file)).stream(), createGunzip(), createWriteStream(localPath));
}

export async function downloadGzFile(file: string, localPath: string) {
  await pipeline(s3Client.file(s3KeyFromUri(file)).stream(), createWriteStream(localPath));
}

export async function uploadGzFile(s3Key: string, localGzPath: string) {
  const writer = s3Client.file(s3Key).writer();
  for await (const chunk of createReadStream(localGzPath)) {
    await writer.write(chunk);
  }
  await writer.end();
}

export async function uploadJsonAsGz(s3Key: string, localJsonPath: string) {
  const writer = s3Client.file(s3Key).writer();
  for await (const chunk of createReadStream(localJsonPath).pipe(createGzip())) {
    await writer.write(chunk);
  }
  await writer.end();
}

export async function readFileLines(localPath: string): Promise<{
  validLines: string[];
  report?: BrokenFileReport;
}> {
  const stream = createReadStream(localPath).pipe(split2());
  const validLines: string[] = [];
  let lineNumber = 0;
  let firstError: { lineNumber: number; line: string; error: string } | undefined;

  for await (const line of stream) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    lineNumber++;
    try {
      JSON.parse(trimmed);
      validLines.push(trimmed);
    } catch (error) {
      if (!firstError) {
        firstError = { lineNumber, line: trimmed, error: String(error) };
      }
    }
  }

  if (!firstError) {
    return { validLines };
  }

  const kind: BrokenFileKind =
    firstError.lineNumber === lineNumber ? "broken_at_end" : "broken_mid_file";

  return {
    validLines,
    report: {
      file: localPath,
      kind,
      lineNumber: firstError.lineNumber,
      line: firstError.line,
      error: firstError.error,
    },
  };
}

export async function classifyBrokenFile(file: string, localPath: string): Promise<BrokenFileReport> {
  const { report } = await readFileLines(localPath);
  if (!report) {
    throw new Error(`File parsed line-by-line without error: ${file}`);
  }
  return { ...report, file };
}

export async function getFirstJsonLine(localPath: string): Promise<Record<string, unknown> | undefined> {
  const stream = createReadStream(localPath).pipe(split2());
  for await (const line of stream) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      return JSON.parse(trimmed) as Record<string, unknown>;
    } catch {
      return undefined;
    }
  }
  return undefined;
}