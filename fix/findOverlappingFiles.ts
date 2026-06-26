import { DuckDBConnection } from "@duckdb/node-api";
import { setupLakehouseConnection } from "../lakehouse/connection.ts";
import { streams } from "../lakehouse/utils.ts";

const FILE_BATCH_SIZE = 10;

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

function sqlString(value: string) {
  return value.replaceAll("'", "''");
}

interface FileTimepointRange {
  file: string;
  index: number;
  min: number;
  max: number;
}

interface DuplicateTimepointRange {
  min: number;
  max: number;
}

async function getDuplicateTimepointRange(
  connection: DuckDBConnection,
): Promise<DuplicateTimepointRange | undefined> {
  console.log("Finding duplicate timepoints in lakehouse.events...");
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
  console.log(`Found duplicate timepoints in lakehouse.events: ${min} - ${max}`);
  return { min: Number(min), max: Number(max) };
}

function fileMatchesDuplicateRange(range: FileTimepointRange, duplicateRange: DuplicateTimepointRange) {
  return range.min === duplicateRange.min && range.max === duplicateRange.max;
}

async function getLoadedFiles(connection: DuckDBConnection, streamPath: string): Promise<string[]> {
  const streamSegment = `/${streamPath}/`;
  const res = await connection.runAndReadAll(`
    SELECT file
    FROM catalogue.cc_metadata.loaded_files
    WHERE contains(file, '${sqlString(streamSegment)}')
      AND ends_with(file, '.json.gz')
    ORDER BY file ASC;
  `);
  return res.getRowObjects().map((row) => row.file as string);
}

async function getUnloadedPrecedingSinkFiles(
  connection: DuckDBConnection,
  streamPath: string,
  latestLoadedFile: string,
): Promise<string[]> {
  const sinkBucket = process.env.SINK_BUCKET;
  if (!sinkBucket) {
    throw new Error("SINK_BUCKET environment variable is required to scan the sink bucket.");
  }

  const res = await connection.runAndReadAll(`
    SELECT file
    FROM glob('s3://${sqlString(sinkBucket)}/${sqlString(streamPath)}/*.json.gz')
    WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files)
      AND file < '${sqlString(latestLoadedFile)}'
    ORDER BY file ASC;
  `);
  return res.getRowObjects().map((row) => row.file as string);
}

function reportUnloadedPrecedingSinkFiles(files: string[], latestLoadedFile: string) {
  console.log(`\nUnloaded sink files preceding latest loaded file (${latestLoadedFile}):`);
  if (files.length === 0) {
    console.log("  None found.");
    return;
  }

  console.log(`  Found ${files.length} file(s) in the sink bucket that are not in cc_metadata.loaded_files:`);
  for (const file of files) {
    console.log(`    ${file}`);
  }
}

async function getFileMinTimepoint(connection: DuckDBConnection, file: string): Promise<number | undefined> {
  const res = await connection.runAndReadAll(`
    SELECT event.timepoint AS min_timepoint
    FROM read_json('${sqlString(file)}')
    WHERE event.timepoint IS NOT NULL
    LIMIT 1;
  `);
  const value = res.getRowObjects()[0]?.min_timepoint;
  return value === null || value === undefined ? undefined : Number(value);
}

async function getFileRanges(
  connection: DuckDBConnection,
  files: string[],
  startIndex: number,
): Promise<FileTimepointRange[]> {
  if (files.length === 0) return [];

  const fileList = files.map((file) => `'${sqlString(file)}'`).join(", ");
  console.log(
    `Reading full timepoint ranges for ${files.length} file(s) ` +
      `(indices ${startIndex + 1}–${startIndex + files.length} of loaded files)...`,
  );

  const res = await connection.runAndReadAll(`
    SELECT
      filename AS file,
      MIN(event.timepoint) AS min_timepoint,
      MAX(event.timepoint) AS max_timepoint
    FROM read_json([${fileList}])
    WHERE event.timepoint IS NOT NULL
    GROUP BY filename;
  `);

  const ranges: FileTimepointRange[] = [];
  for (const row of res.getRowObjects()) {
    const file = row.file as string;
    ranges.push({
      file,
      index: startIndex + files.indexOf(file),
      min: Number(row.min_timepoint),
      max: Number(row.max_timepoint),
    });
  }

  return ranges.toSorted((a, b) => a.index - b.index);
}

async function findDuplicateBatchStart(
  connection: DuckDBConnection,
  loadedFiles: string[],
  duplicateTimepoint: number,
): Promise<number> {
  console.log(
    `Scanning backwards from most recent file (quick min timepoint checks, step ${FILE_BATCH_SIZE})...`,
  );

  for (let offset = 1; offset <= loadedFiles.length; offset += FILE_BATCH_SIZE) {
    const index = loadedFiles.length - offset;
    const file = loadedFiles[index]!;
    const min = await getFileMinTimepoint(connection, file);

    if (min === undefined) {
      console.log(
        `  File ${index + 1}/${loadedFiles.length} (${offset} from end): no timepoints — skipping`,
      );
      continue;
    }

    console.log(
      `  File ${index + 1}/${loadedFiles.length} (${offset} from end): min timepoint ${min}` +
        (min < duplicateTimepoint ? " — before duplicate region, stopping scan" : ""),
    );

    if (min < duplicateTimepoint) {
      return index;
    }
  }

  console.log("  Reached oldest loaded file without finding min below duplicate timepoint.");
  return 0;
}

function overlapContainsTimepoint(
  earlier: FileTimepointRange,
  later: FileTimepointRange,
  duplicateTimepoint: number,
) {
  return (
    earlier.max >= later.min &&
    duplicateTimepoint >= later.min &&
    duplicateTimepoint <= earlier.max
  );
}

function rangesOverlap(earlier: FileTimepointRange, later: FileTimepointRange) {
  return earlier.max >= later.min;
}

function findOverlappingPair(
  ranges: FileTimepointRange[],
  duplicateTimepoint: number,
): [FileTimepointRange, FileTimepointRange] | undefined {
  for (let laterIndex = ranges.length - 1; laterIndex >= 0; laterIndex--) {
    const later = ranges[laterIndex]!;
    if (later.max < duplicateTimepoint) continue;

    for (let earlierIndex = laterIndex - 1; earlierIndex >= 0; earlierIndex--) {
      const earlier = ranges[earlierIndex]!;
      if (overlapContainsTimepoint(earlier, later, duplicateTimepoint)) {
        return [earlier, later];
      }
      if (earlier.max < later.min) break;
    }
  }

  return undefined;
}

function printBatchRanges(
  ranges: FileTimepointRange[],
  duplicateRange: DuplicateTimepointRange,
  batchStart: number,
) {
  console.log("\nFiles in duplicate region batch:");
  for (let i = 0; i < ranges.length; i++) {
    const range = ranges[i]!;
    const overlapsWithPrevious = i > 0 && rangesOverlap(ranges[i - 1]!, range);
    const containsDuplicate =
      range.min <= duplicateRange.max && range.max >= duplicateRange.min;
    const potentiallyLoadedTwice = fileMatchesDuplicateRange(range, duplicateRange);
    const flags = [
      range.index < batchStart ? "preceding context" : null,
      overlapsWithPrevious ? "OVERLAPS previous file" : null,
      containsDuplicate ? "contains duplicate timepoint(s)" : null,
      potentiallyLoadedTwice ? "POTENTIALLY LOADED TWICE — file min/max exactly matches duplicate range" : null,
    ].filter(Boolean);

    console.log(`  [${range.index + 1}] ${range.file}`);
    console.log(`    timepoints: ${range.min} – ${range.max}`);
    if (flags.length > 0) {
      console.log(`    ** ${flags.join("; ")} **`);
    }
  }
}

function findPotentiallyLoadedTwice(
  ranges: FileTimepointRange[],
  duplicateRange: DuplicateTimepointRange,
) {
  return ranges.filter((range) => fileMatchesDuplicateRange(range, duplicateRange));
}

function findRangeByIndex(ranges: FileTimepointRange[], index: number) {
  return ranges.find((file) => file.index === index);
}

function printFile(label: string, file: FileTimepointRange | undefined) {
  console.log(`  ${label}:`);
  if (!file) {
    console.log("    n/a");
    return;
  }
  console.log(`    ${file.file}`);
  console.log(`    timepoints: ${file.min} – ${file.max}`);
}

function recommendDeletion(
  duplicateTimepoint: number,
  before: FileTimepointRange | undefined,
  earlierOverlap: FileTimepointRange,
  laterOverlap: FileTimepointRange,
  after: FileTimepointRange | undefined,
) {
  const expectedMin = before ? before.max + 1 : earlierOverlap.min;
  const expectedMax = after ? after.min - 1 : laterOverlap.max;

  const earlierFillsGap =
    earlierOverlap.min === expectedMin && earlierOverlap.max === expectedMax;
  const laterFillsGap = laterOverlap.min === expectedMin && laterOverlap.max === expectedMax;

  const laterLooksLikeRecapture = laterOverlap.min <= earlierOverlap.max;
  const earlierLooksLikeRecapture = earlierOverlap.min <= (before?.max ?? earlierOverlap.min - 1);

  let likelyCorrect = earlierOverlap;
  let likelyWrong = laterOverlap;
  let reason =
    "Later file (higher uuidv7) starts at or before the earlier file ends, which usually indicates a re-capture overlap.";

  if (laterFillsGap && !earlierFillsGap) {
    likelyCorrect = laterOverlap;
    likelyWrong = earlierOverlap;
    reason = "Later file exactly fills the gap between the files before and after the overlap.";
  } else if (earlierFillsGap && !laterFillsGap) {
    reason = "Earlier file exactly fills the gap between the files before and after the overlap.";
  } else if (laterLooksLikeRecapture) {
    // default: delete later re-capture
  } else if (earlierLooksLikeRecapture && !laterLooksLikeRecapture) {
    likelyCorrect = laterOverlap;
    likelyWrong = earlierOverlap;
    reason = "Earlier file extends back before the preceding file ends.";
  } else {
    reason =
      "//TODO: check this - neither overlapping file exactly fills the before/after gap; inspect ranges manually.";
  }

  return {
    duplicateTimepoint,
    likelyCorrect,
    likelyWrong,
    reason,
    expectedRange: { min: expectedMin, max: expectedMax },
  };
}

function printReport(
  streamPath: string,
  duplicateRange: DuplicateTimepointRange,
  before: FileTimepointRange | undefined,
  earlierOverlap: FileTimepointRange,
  laterOverlap: FileTimepointRange,
  after: FileTimepointRange | undefined,
) {
  const recommendation = recommendDeletion(
    duplicateRange.min,
    before,
    earlierOverlap,
    laterOverlap,
    after,
  );

  console.log(`\nOverlap found in stream: ${streamPath}`);
  console.log(
    `Duplicate lakehouse timepoint range: ${duplicateRange.min}` +
      (duplicateRange.min === duplicateRange.max ? "" : ` – ${duplicateRange.max}`) +
      "\n",
  );

  console.log("Context files:");
  printFile("Before overlap", before);
  printFile("Overlapping file A (earlier uuidv7)", earlierOverlap);
  printFile("Overlapping file B (later uuidv7)", laterOverlap);
  printFile("After overlap", after);

  console.log("\nContiguity check:");
  if (before) {
    console.log(
      `  Before ends at ${before.max}; earlier overlap starts at ${earlierOverlap.min} (gap: ${earlierOverlap.min - before.max - 1})`,
    );
  }
  console.log(
    `  Earlier overlap ends at ${earlierOverlap.max}; later overlap starts at ${laterOverlap.min} (overlap width: ${earlierOverlap.max - laterOverlap.min + 1})`,
  );
  if (after) {
    console.log(
      `  Later overlap ends at ${laterOverlap.max}; after starts at ${after.min} (gap: ${after.min - laterOverlap.max - 1})`,
    );
  }

  console.log("\nRecommendation:");
  console.log(
    `  Expected contiguous range between before/after: ${recommendation.expectedRange.min} – ${recommendation.expectedRange.max}`,
  );
  console.log(`  Likely correct file (keep):\n    ${recommendation.likelyCorrect.file}`);
  console.log(`    timepoints: ${recommendation.likelyCorrect.min} – ${recommendation.likelyCorrect.max}`);
  console.log(`  Likely wrong file (delete from sink and lakehouse metadata):\n    ${recommendation.likelyWrong.file}`);
  console.log(`    timepoints: ${recommendation.likelyWrong.min} – ${recommendation.likelyWrong.max}`);
  console.log(`  Reason: ${recommendation.reason}`);
}

async function main(streamPath: string) {
  if (!streamPath) {
    throw new Error("No stream provided. Usage: bun lakehouse/findOverlappingFiles.ts [stream]");
  }
  if (!streams.includes(streamPath)) {
    throw new Error(`Invalid stream. Options: ${streams.join(", ")}`);
  }

  const { connection } = await setupLakehouseConnection();
  try {
    await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

    const duplicateRange = await getDuplicateTimepointRange(connection);
    if (duplicateRange === undefined) {
      console.log(`No duplicate timepoints in lakehouse.${getSchema(streamPath)}.events — all good.`);
      return;
    }

    const loadedFiles = await getLoadedFiles(connection, streamPath);
    if (loadedFiles.length < 2) {
      throw new Error(`Need at least two loaded files to diagnose overlap, found ${loadedFiles.length}`);
    }

    console.log(
      `Duplicate timepoint range: ${duplicateRange.min}` +
        (duplicateRange.min === duplicateRange.max ? "" : ` – ${duplicateRange.max}`),
    );
    console.log(`Loaded files in sink metadata: ${loadedFiles.length}`);

    const latestLoadedFile = loadedFiles[loadedFiles.length - 1]!;
    const unloadedPrecedingSinkFiles = await getUnloadedPrecedingSinkFiles(
      connection,
      streamPath,
      latestLoadedFile,
    );
    reportUnloadedPrecedingSinkFiles(unloadedPrecedingSinkFiles, latestLoadedFile);

    const batchStart = await findDuplicateBatchStart(connection, loadedFiles, duplicateRange.min);
    const contextStart = batchStart > 0 ? batchStart - 1 : batchStart;
    const batchFiles = loadedFiles.slice(contextStart, batchStart + FILE_BATCH_SIZE);

    console.log(
      `\nDuplicate region likely starts around file ${batchStart + 1}; ` +
        `reading full ranges for ${batchFiles.length} file(s)` +
        (contextStart < batchStart ? ` (including preceding file ${contextStart + 1} for overlap detection)` : "") +
        ".",
    );

    const fileRanges = await getFileRanges(connection, batchFiles, contextStart);
    if (fileRanges.length !== batchFiles.length) {
      console.warn(
        `Warning: expected ${batchFiles.length} file ranges, got ${fileRanges.length}. Some batch files may be unreadable.`,
      );
    }

    printBatchRanges(fileRanges, duplicateRange, batchStart);

    const potentiallyLoadedTwice = findPotentiallyLoadedTwice(fileRanges, duplicateRange);
    if (potentiallyLoadedTwice.length > 0) {
      console.log(
        `\nPossible double-load detected: ${potentiallyLoadedTwice.length} file(s) span exactly ` +
          `the duplicate timepoint range (${duplicateRange.min}` +
          (duplicateRange.min === duplicateRange.max ? "" : ` – ${duplicateRange.max}`) +
          "), suggesting the file may have been ingested twice:",
      );
      for (const range of potentiallyLoadedTwice) {
        console.log(`  ${range.file}`);
      }
    }

    const overlapPair = findOverlappingPair(fileRanges, duplicateRange.min);
    if (!overlapPair) {
      if (potentiallyLoadedTwice.length > 0) {
        console.log(
          "\nNo overlapping file pair found, but a matching file range suggests a double-load rather than a re-capture overlap.",
        );
        console.log(
          "To correct, run: bun lakehouse/correctDoubleLoadedFile.ts <s3-uri> [<s3-uri> ...] [--dry-run | --apply]",
        );
        return;
      }
      throw new Error(
        `Could not find overlapping files for duplicate timepoint ${duplicateRange.min} in the scanned batch. ` +
          "//TODO: check this - expand the batch or inspect files no longer listed in cc_metadata.loaded_files.",
      );
    }

    const [earlierOverlap, laterOverlap] = overlapPair;
    let before = findRangeByIndex(fileRanges, earlierOverlap.index - 1);
    if (!before && earlierOverlap.index > 0) {
      [before] = await getFileRanges(connection, [loadedFiles[earlierOverlap.index - 1]!], earlierOverlap.index - 1);
    }

    let after = findRangeByIndex(fileRanges, laterOverlap.index + 1);
    if (!after && laterOverlap.index < loadedFiles.length - 1) {
      [after] = await getFileRanges(connection, [loadedFiles[laterOverlap.index + 1]!], laterOverlap.index + 1);
    }

    printReport(streamPath, duplicateRange, before, earlierOverlap, laterOverlap, after);
  } finally {
    connection.closeSync();
  }
}

await main(process.argv[2]);