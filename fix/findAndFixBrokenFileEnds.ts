import { basename, join } from "node:path";
import {
  classifyBrokenFile,
  downloadFile,
  getAllBucketFiles,
  getUnloadedFiles,
  isValidJsonFile,
} from "./brokenFileUtils.ts";
import {
  getNextFile,
  tryFixFile,
  validateTimepointContinuity,
  withTempDir,
} from "./fixBrokenEndFiles.ts";
import { setupLakehouseConnection } from "../lakehouse/connection.ts";
import { streams } from "../lakehouse/utils.ts";

type FixOutcome =
  | { file: string; status: "fixed" }
  | { file: string; status: "skipped_mid_file"; lineNumber: number; line: string; error: string }
  | { file: string; status: "failed"; reason: string };

async function analyseBrokenFile(file: string) {
  return withTempDir("findAndFixBrokenFileEnds-check-", async (tmpDir) => {
    const localPath = join(tmpDir, basename(file).replace(/\.gz$/, ""));
    await downloadFile(file, localPath);
    return classifyBrokenFile(file, localPath);
  });
}

async function fixEndBrokenFile(
  connection: Awaited<ReturnType<typeof setupLakehouseConnection>>["connection"],
  streamPath: string,
  file: string,
  allFiles: string[],
): Promise<FixOutcome> {
  const backupDir = join(import.meta.dir, "backups", streamPath);

  const nextFile = getNextFile(allFiles, file);
  if (nextFile) {
    const { lastTimepoint, nextTimepoint } = await validateTimepointContinuity(file, nextFile);
    console.log(
      `Timepoint continuity ok for ${file}: ${lastTimepoint} -> ${nextTimepoint} (via ${nextFile})`,
    );
  } else {
    console.log("No following file to verify continuity");
  }

  const fixed = await tryFixFile(connection, file, backupDir);
  if (!fixed) {
    return {
      file,
      status: "failed",
      reason: "DuckDB verification failed after upload; original restored",
    };
  }

  return { file, status: "fixed" };
}

async function processStream(
  connection: Awaited<ReturnType<typeof setupLakehouseConnection>>["connection"],
  streamPath: string,
): Promise<FixOutcome[]> {
  const outcomes: FixOutcome[] = [];

  console.log("\n=== Stream:", streamPath, "===");
  const unloadedFiles = await getUnloadedFiles(connection, streamPath);
  console.log("Unloaded files to inspect:", unloadedFiles.length);

  if (unloadedFiles.length === 0) {
    return outcomes;
  }

  const allFiles = await getAllBucketFiles(connection, streamPath);

  for (const file of unloadedFiles) {
    process.stdout.write(`Checking ${file}... `);
    const valid = await isValidJsonFile(connection, file);
    if (valid) {
      console.log("ok");
      continue;
    }

    console.log("INVALID");
    console.log("Analysing broken file...");
    const report = await analyseBrokenFile(file);

    if (report.kind !== "broken_at_end") {
      console.log(
        `Skipping ${file}: broken mid file at line ${report.lineNumber}\n` +
          `Content: ${report.line}\n` +
          `Error: ${report.error}`,
      );
      outcomes.push({
        file,
        status: "skipped_mid_file",
        lineNumber: report.lineNumber,
        line: report.line,
        error: report.error,
      });
      continue;
    }

    console.log(`Broken only at end of file (line ${report.lineNumber}). Applying fix...`);

    try {
      const outcome = await fixEndBrokenFile(connection, streamPath, file, allFiles);
      outcomes.push(outcome);
      if (outcome.status === "fixed") {
        console.log("Fixed:", file);
      } else {
        console.error("Failed to fix:", file, "-", outcome.reason);
      }
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      console.error("Failed to fix:", file, "-", reason);
      outcomes.push({ file, status: "failed", reason });
    }
  }

  return outcomes;
}

async function main() {
  const { connection } = await setupLakehouseConnection();
  const allOutcomes: FixOutcome[] = [];

  try {
    console.log("Finding and fixing end-broken files across all streams...");
    console.log("Streams:", streams.join(", "));

    for (const streamPath of streams) {
      const outcomes = await processStream(connection, streamPath);
      allOutcomes.push(...outcomes);
    }

    const fixed = allOutcomes.filter((o) => o.status === "fixed");
    const skipped = allOutcomes.filter((o) => o.status === "skipped_mid_file");
    const failed = allOutcomes.filter((o) => o.status === "failed");

    console.log("\n=== Summary ===");
    console.log("Fixed (broken at end):", fixed.length);
    for (const o of fixed) console.log("  -", o.file);

    console.log("Skipped (broken mid file):", skipped.length);
    for (const o of skipped) {
      console.log(`  - ${o.file} (line ${o.lineNumber}): ${o.error}`);
    }

    console.log("Failed:", failed.length);
    for (const o of failed) console.log(`  - ${o.file}: ${o.reason}`);

    if (failed.length > 0) {
      throw new Error(`${failed.length} file(s) failed to fix`);
    }

    console.log("Done.");
  } finally {
    connection.closeSync();
  }
}

await main();
