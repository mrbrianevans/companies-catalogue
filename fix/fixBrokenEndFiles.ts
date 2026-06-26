import { DuckDBConnection } from "@duckdb/node-api";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { getLastJsonLine } from "../eventCapture/utils.ts";
import {
  analyseFileLines,
  classifyBrokenFile,
  downloadFile,
  downloadGzFile,
  getAllBucketFiles,
  getFirstJsonLine,
  isValidJsonFile,
  s3KeyFromUri,
  writeValidLines,
  streamPathFromUri,
  uploadGzFile,
  uploadJsonAsGz,
  validateS3FileUri,
} from "./brokenFileUtils.ts";
import { setupLakehouseConnection } from "../lakehouse/connection.ts";
import { streams } from "../lakehouse/utils.ts";

function getEventTimepoint(event: Record<string, unknown> | undefined): number | undefined {
  const nested = event?.event;
  if (!nested || typeof nested !== "object") return undefined;
  const timepoint = (nested as Record<string, unknown>).timepoint;
  return typeof timepoint === "number" ? timepoint : undefined;
}

function getNextFile(allFiles: string[], file: string): string | undefined {
  const index = allFiles.indexOf(file);
  if (index < 0 || index >= allFiles.length - 1) return undefined;
  return allFiles[index + 1];
}

async function withTempDir<T>(prefix: string, fn: (dir: string) => Promise<T>): Promise<T> {
  const tmpDir = await mkdtemp(join(tmpdir(), prefix));
  try {
    return await fn(tmpDir);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
}

async function validateTimepointContinuity(
  file: string,
  nextFile: string,
): Promise<{ lastTimepoint: number; nextTimepoint: number }> {
  return withTempDir("fixBrokenEndFiles-validate-", async (tmpDir) => {
    const currentLocalPath = join(tmpDir, basename(file).replace(/\.gz$/, ""));
    const nextLocalPath = join(tmpDir, basename(nextFile).replace(/\.gz$/, ""));

    await downloadFile(file, currentLocalPath);
    await downloadFile(nextFile, nextLocalPath);

    const lastEvent = await getLastJsonLine(currentLocalPath);
    const firstEvent = await getFirstJsonLine(nextLocalPath);
    const lastTimepoint = getEventTimepoint(lastEvent);
    const nextTimepoint = getEventTimepoint(firstEvent);

    if (lastTimepoint === undefined) {
      throw new Error(`Could not read last whole event timepoint from ${file}`);
    }
    if (nextTimepoint === undefined) {
      throw new Error(`Could not read first event timepoint from ${nextFile}`);
    }
    if (nextTimepoint !== lastTimepoint + 1) {
      throw new Error(
        `Timepoint continuity check failed for ${file}: ` +
          `last whole event timepoint ${lastTimepoint}, ` +
          `next file first event timepoint ${nextTimepoint}`,
      );
    }

    return { lastTimepoint, nextTimepoint };
  });
}

async function saveOriginalBackup(file: string, backupDir: string): Promise<string> {
  await mkdir(backupDir, { recursive: true });
  const backupPath = join(backupDir, basename(file));
  await downloadGzFile(file, backupPath);
  console.log("Saved original backup to", backupPath);
  return backupPath;
}

async function buildFixedFile(file: string, fixedLocalPath: string) {
  await withTempDir("fixBrokenEndFiles-build-", async (tmpDir) => {
    const originalLocalPath = join(tmpDir, basename(file).replace(/\.gz$/, ""));
    await downloadFile(file, originalLocalPath);
    const { report } = await analyseFileLines(originalLocalPath);
    if (!report || report.kind !== "broken_at_end") {
      throw new Error(`Expected broken_at_end file: ${file}`);
    }
    await writeValidLines(originalLocalPath, fixedLocalPath);
  });
}

async function tryFixFile(
  connection: DuckDBConnection,
  file: string,
  backupDir: string,
): Promise<boolean> {
  const s3Key = s3KeyFromUri(file);
  const backupPath = await saveOriginalBackup(file, backupDir);

  return withTempDir("fixBrokenEndFiles-fix-", async (tmpDir) => {
    const fixedLocalPath = join(tmpDir, basename(file).replace(/\.gz$/, ""));
    await buildFixedFile(file, fixedLocalPath);

    console.log("Uploading fixed file to", file);
    await uploadJsonAsGz(s3Key, fixedLocalPath);

    const valid = await isValidJsonFile(connection, file);
    if (valid) {
      console.log("Fix verified successfully for", file);
      return true;
    }

    console.error("DuckDB verification failed for fixed file, restoring original:", file);
    await uploadGzFile(s3Key, backupPath);
    console.log("Original file restored from backup for", file);
    return false;
  });
}

async function main(fileUri: string) {
  validateS3FileUri(fileUri);

  const streamPath = streamPathFromUri(fileUri);
  if (!streams.includes(streamPath)) {
    throw new Error(`Invalid stream in URI. Options: ${streams.join(", ")}`);
  }

  const backupDir = join(import.meta.dir, "backups", streamPath);
  const { connection } = await setupLakehouseConnection();

  try {
    console.log("Fixing end-broken file:", fileUri);

    const valid = await isValidJsonFile(connection, fileUri);
    if (valid) {
      console.log("File is already valid, nothing to fix.");
      return;
    }

    console.log("File is invalid, analysing...");
    const report = await withTempDir("fixBrokenEndFiles-check-", async (tmpDir) => {
      const localPath = join(tmpDir, basename(fileUri).replace(/\.gz$/, ""));
      await downloadFile(fileUri, localPath);
      return classifyBrokenFile(fileUri, localPath);
    });

    if (report.kind !== "broken_at_end") {
      throw new Error(
        `Cannot fix ${fileUri}: broken mid file at line ${report.lineNumber}\n` +
          `Content: ${report.line}\n` +
          `Error: ${report.error}`,
      );
    }

    const allFiles = await getAllBucketFiles(connection, streamPath);
    const nextFile = getNextFile(allFiles, fileUri);
    if (!nextFile) {
      throw new Error(
        `Cannot fix ${fileUri}: no following file in bucket to verify timepoint continuity`,
      );
    }

    const { lastTimepoint, nextTimepoint } = await validateTimepointContinuity(fileUri, nextFile);
    console.log(
      `Timepoint continuity ok for ${fileUri}: ${lastTimepoint} -> ${nextTimepoint} (via ${nextFile})`,
    );

    const fixed = await tryFixFile(connection, fileUri, backupDir);
    if (!fixed) {
      throw new Error(`Failed to fix ${fileUri}; original file has been restored`);
    }

    console.log("Done.");
  } finally {
    connection.closeSync();
  }
}

await main(process.argv[2]);
