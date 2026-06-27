import { DuckDBConnection } from "@duckdb/node-api";
import { saveAndCloseLakehouse, setupLakehouseConnection } from "../lakehouse/connection.ts";
import { streamPathFromUri, validateS3FileUri } from "./brokenFileUtils.ts";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

function sqlString(value: string) {
  return value.replaceAll("'", "''");
}

interface CliOptions {
  files: string[];
  dryRun: boolean;
}

interface FileTimepointRange {
  file: string;
  min: number;
  max: number;
}

interface DuplicateTimepointRange {
  min: number;
  max: number;
}

interface CorrectionPlan {
  files: FileTimepointRange[];
  duplicateRange: DuplicateTimepointRange;
  eventRowsToDelete: number;
  distinctTimepointsInRange: number;
  duplicateTimepointsInRange: number;
}

function parseArgs(argv: string[]): CliOptions {
  const args = argv.slice(2).filter((arg) => arg !== "--help" && arg !== "-h");
  const files = args.filter((arg) => !arg.startsWith("-"));
  const dryRun = !args.includes("--apply");
  return { files, dryRun };
}

function printUsage() {
  console.log(
    "Usage: bun lakehouse/correctDoubleLoadedFile.ts <s3-uri> [<s3-uri> ...] [--dry-run | --apply]",
  );
  console.log("  <s3-uri>    One or more S3 paths of double-loaded sink files");
  console.log("  --dry-run   Preview corrections without applying (default)");
  console.log("  --apply     Delete duplicate events and unload files from metadata");
}

function formatRange(range: DuplicateTimepointRange) {
  return `${range.min}` + (range.min === range.max ? "" : ` – ${range.max}`);
}

function combinedRange(files: FileTimepointRange[]): DuplicateTimepointRange {
  return {
    min: Math.min(...files.map((file) => file.min)),
    max: Math.max(...files.map((file) => file.max)),
  };
}

async function getFileTimepointRanges(
  connection: DuckDBConnection,
  files: string[],
): Promise<FileTimepointRange[]> {
  const fileList = files.map((file) => `'${sqlString(file)}'`).join(", ");
  const res = await connection.runAndReadAll(`
    SELECT
      filename AS file,
      MIN(event.timepoint) AS min_timepoint,
      MAX(event.timepoint) AS max_timepoint
    FROM read_json([${fileList}])
    WHERE event.timepoint IS NOT NULL
    GROUP BY filename;
  `);

  const ranges = new Map<string, FileTimepointRange>();
  for (const row of res.getRowObjects()) {
    const file = row.file as string;
    const min = row.min_timepoint;
    const max = row.max_timepoint;
    if (min === null || min === undefined || max === null || max === undefined) {
      throw new Error(`No timepoints found in file: ${file}`);
    }
    ranges.set(file, { file, min: Number(min), max: Number(max) });
  }

  const missing = files.filter((file) => !ranges.has(file));
  if (missing.length > 0) {
    throw new Error(`Could not read timepoint ranges for: ${missing.join(", ")}`);
  }

  return files.map((file) => ranges.get(file)!);
}

async function getDuplicateTimepointRange(
  connection: DuckDBConnection,
): Promise<DuplicateTimepointRange | undefined> {
  const res = await connection.runAndReadAll(`
    SELECT
      MIN(timepoint) AS min_duplicate_timepoint,
      MAX(timepoint) AS max_duplicate_timepoint
    FROM (
      SELECT event.timepoint AS timepoint
      FROM events
      GROUP BY event.timepoint
      HAVING COUNT(*) > 1
    );
  `);
  const row = res.getRowObjects()[0];
  const min = row?.min_duplicate_timepoint;
  const max = row?.max_duplicate_timepoint;
  if (min === null || min === undefined || max === null || max === undefined) {
    return undefined;
  }
  return { min: Number(min), max: Number(max) };
}

function assertFilesContiguous(files: FileTimepointRange[]) {
  const sorted = files.toSorted((a, b) => a.min - b.min);
  for (let i = 1; i < sorted.length; i++) {
    const earlier = sorted[i - 1]!;
    const later = sorted[i]!;
    if (later.min !== earlier.max + 1) {
      throw new Error(
        `Files are not contiguous: ${earlier.file} ends at ${earlier.max}, ` +
          `but ${later.file} starts at ${later.min}.`,
      );
    }
  }
}

async function confirmDoubleLoaded(
  connection: DuckDBConnection,
  fileRanges: FileTimepointRange[],
): Promise<DuplicateTimepointRange> {
  const duplicateRange = await getDuplicateTimepointRange(connection);
  if (duplicateRange === undefined) {
    throw new Error("No duplicate timepoints found in lakehouse.events.");
  }

  const fileRange = combinedRange(fileRanges);
  const rangesMatch = fileRange.min === duplicateRange.min && fileRange.max === duplicateRange.max;
  if (!rangesMatch) {
    throw new Error(
      `Combined file timepoint range (${formatRange(fileRange)}) does not exactly match the duplicate ` +
        `timepoint range in events (${formatRange(duplicateRange)}). ` +
        "These files do not appear to cover the double-loaded region.",
    );
  }

  if (fileRanges.length > 1) {
    assertFilesContiguous(fileRanges);
  }

  const duplicateCountRes = await connection.runAndReadAll(`
    SELECT COUNT(*) AS duplicate_timepoints
    FROM (
      SELECT event.timepoint AS timepoint
      FROM events
      WHERE event.timepoint >= ${fileRange.min}
        AND event.timepoint <= ${fileRange.max}
      GROUP BY event.timepoint
      HAVING COUNT(*) > 1
    );
  `);
  const duplicateTimepointsInRange = Number(
    duplicateCountRes.getRowObjects()[0]!.duplicate_timepoints,
  );
  const expectedTimepoints = fileRange.max - fileRange.min + 1;
  if (duplicateTimepointsInRange !== expectedTimepoints) {
    throw new Error(
      `Expected all ${expectedTimepoints} timepoints in the combined file range to be duplicated, ` +
        `but only ${duplicateTimepointsInRange} are.`,
    );
  }

  const fileList = fileRanges.map((file) => `'${sqlString(file.file)}'`).join(", ");
  const loadedRes = await connection.runAndReadAll(`
    SELECT file
    FROM catalogue.cc_metadata.loaded_files
    WHERE file IN (${fileList});
  `);
  const loadedFiles = new Set(loadedRes.getRowObjects().map((row) => row.file as string));
  const missingFromMetadata = fileRanges
    .map((file) => file.file)
    .filter((file) => !loadedFiles.has(file));
  if (missingFromMetadata.length > 0) {
    throw new Error(
      `File(s) not in catalogue.cc_metadata.loaded_files:\n  ${missingFromMetadata.join("\n  ")}`,
    );
  }

  console.log(
    `Confirmed double-loaded file${fileRanges.length === 1 ? "" : "s"} ` +
      `(${fileRanges.length} file${fileRanges.length === 1 ? "" : "s"}):`,
  );
  for (const range of fileRanges.toSorted((a, b) => a.min - b.min)) {
    console.log(`  ${range.file}`);
    console.log(`    timepoints: ${formatRange(range)}`);
  }
  console.log(`  Combined range: ${formatRange(fileRange)}`);
  console.log(`  Duplicate timepoints in range: ${duplicateTimepointsInRange}`);

  return duplicateRange;
}

async function buildCorrectionPlan(
  connection: DuckDBConnection,
  fileRanges: FileTimepointRange[],
  duplicateRange: DuplicateTimepointRange,
): Promise<CorrectionPlan> {
  const res = await connection.runAndReadAll(`
    SELECT
      COUNT(*) AS event_rows_to_delete,
      COUNT(DISTINCT event.timepoint) AS distinct_timepoints_in_range
    FROM events
    WHERE event.timepoint >= ${duplicateRange.min}
      AND event.timepoint <= ${duplicateRange.max};
  `);
  const row = res.getRowObjects()[0]!;

  return {
    files: fileRanges,
    duplicateRange,
    eventRowsToDelete: Number(row.event_rows_to_delete),
    distinctTimepointsInRange: Number(row.distinct_timepoints_in_range),
    duplicateTimepointsInRange: duplicateRange.max - duplicateRange.min + 1,
  };
}

function printCorrectionPlan(plan: CorrectionPlan, dryRun: boolean) {
  const { duplicateRange, files } = plan;

  console.log(`\nCorrection plan${dryRun ? " (dry run)" : ""}:`);
  console.log(`  File${files.length === 1 ? "" : "s"} to unload from metadata (${files.length}):`);
  for (const file of files.toSorted((a, b) => a.min - b.min)) {
    console.log(`    ${file.file}`);
  }
  console.log(`  Timepoint range to delete from events: ${formatRange(duplicateRange)}`);
  console.log(`  Event rows to delete: ${plan.eventRowsToDelete}`);
  console.log(
    `  Distinct timepoints in range: ${plan.distinctTimepointsInRange} ` +
      `(expected ${plan.duplicateTimepointsInRange} after re-load)`,
  );

  console.log("\nActions:");
  console.log(
    `  1. DELETE FROM events WHERE event.timepoint BETWEEN ${duplicateRange.min} AND ${duplicateRange.max}`,
  );
  console.log(
    `  2. DELETE FROM catalogue.cc_metadata.loaded_files WHERE file IN (${files.length} file${files.length === 1 ? "" : "s"})`,
  );
  console.log(
    `  3. Re-run lakehousing to reload ${files.length === 1 ? "the file" : "the files"} once`,
  );

  if (dryRun) {
    console.log("\nDry run only — no changes applied. Re-run with --apply to execute.");
  }
}

async function applyCorrection(
  connection: DuckDBConnection,
  plan: CorrectionPlan,
  dryRun: boolean,
) {
  printCorrectionPlan(plan, dryRun);

  if (plan.eventRowsToDelete === 0) {
    throw new Error("No events found in the duplicate timepoint range — nothing to delete.");
  }

  if (dryRun) {
    return;
  }

  const fileList = plan.files.map((file) => `'${sqlString(file.file)}'`).join(", ");

  console.log("\nApplying correction...");
  await connection.run(`
    DELETE FROM events
    WHERE event.timepoint >= ${plan.duplicateRange.min}
      AND event.timepoint <= ${plan.duplicateRange.max};
  `);
  await connection.run(`
    DELETE FROM catalogue.cc_metadata.loaded_files
    WHERE file IN (${fileList});
  `);

  const remainingDuplicates = await getDuplicateTimepointRange(connection);
  if (remainingDuplicates !== undefined) {
    console.warn(
      "Warning: duplicate timepoints remain after correction:",
      formatRange(remainingDuplicates),
    );
  } else {
    console.log("No duplicate timepoints remain in events.");
  }

  await saveAndCloseLakehouse({ connection });
  console.log("Correction applied and lakehouse changes saved.");
}

async function main(options: CliOptions) {
  const { files, dryRun } = options;
  if (files.length === 0) {
    printUsage();
    throw new Error("No S3 file URI(s) provided.");
  }

  for (const file of files) {
    validateS3FileUri(file);
  }

  const streamPaths = new Set(files.map((file) => streamPathFromUri(file)));
  if (streamPaths.size > 1) {
    throw new Error(
      `All files must belong to the same stream, found: ${[...streamPaths].join(", ")}`,
    );
  }

  const streamPath = [...streamPaths][0]!;
  const { connection } = await setupLakehouseConnection();

  try {
    await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

    const fileRanges = await getFileTimepointRanges(connection, files);
    const duplicateRange = await confirmDoubleLoaded(connection, fileRanges);
    const plan = await buildCorrectionPlan(connection, fileRanges, duplicateRange);
    await applyCorrection(connection, plan, dryRun);
  } finally {
    connection.closeSync();
  }
}

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  printUsage();
} else {
  await main(parseArgs(process.argv));
}
