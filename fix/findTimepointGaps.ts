import { setupLakehouseConnection } from "../lakehouse/connection.ts";
import { streams } from "../lakehouse/utils.ts";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

interface TimepointRange {
  kind: "missing" | "duplicate";
  min: number;
  max: number;
}

function formatRange(min: number, max: number) {
  return min === max ? `${min}` : `${min} – ${max}`;
}

async function findMissingRanges(streamPath: string) {
  const { connection } = await setupLakehouseConnection();
  try {
    await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

    const boundsRes = await connection.runAndReadAll(`
      SELECT
        MIN(event.timepoint) AS min_timepoint,
        MAX(event.timepoint) AS max_timepoint,
        COUNT(*) AS row_count,
        COUNT(DISTINCT event.timepoint) AS distinct_timepoints
      FROM events;
    `);
    const bounds = boundsRes.getRowObjects()[0]!;
    const min = bounds.min_timepoint;
    const max = bounds.max_timepoint;

    if (min === null || min === undefined || max === null || max === undefined) {
      console.log("No events in lakehouse.events.");
      return;
    }

    const minTimepoint = Number(min);
    const maxTimepoint = Number(max);
    const expectedTimepoints = maxTimepoint - minTimepoint + 1;

    console.log(`Stream: ${streamPath}`);
    console.log(`Timepoint bounds: ${minTimepoint} – ${maxTimepoint}`);
    console.log(`Rows: ${bounds.row_count}, distinct timepoints: ${bounds.distinct_timepoints}`);
    console.log(`Expected distinct timepoints: ${expectedTimepoints}`);

    const missingRes = await connection.runAndReadAll(`
      WITH distinct_timepoints AS (
        SELECT DISTINCT event.timepoint AS timepoint
        FROM events
      ),
      with_previous AS (
        SELECT
          timepoint,
          LAG(timepoint) OVER (ORDER BY timepoint) AS previous_timepoint
        FROM distinct_timepoints
      )
      SELECT
        previous_timepoint + 1 AS range_min,
        timepoint - 1 AS range_max
      FROM with_previous
      WHERE timepoint - previous_timepoint > 1
      ORDER BY range_min;
    `);

    const duplicateRes = await connection.runAndReadAll(`
      WITH duplicate_timepoints AS (
        SELECT event.timepoint AS timepoint
        FROM events
        GROUP BY event.timepoint
        HAVING COUNT(*) > 1
      ),
      grouped AS (
        SELECT
          timepoint,
          timepoint - ROW_NUMBER() OVER (ORDER BY timepoint) AS group_id
        FROM duplicate_timepoints
      )
      SELECT
        MIN(timepoint) AS range_min,
        MAX(timepoint) AS range_max
      FROM grouped
      GROUP BY group_id
      ORDER BY range_min;
    `);

    const issues: TimepointRange[] = [
      ...missingRes.getRowObjects().map((row) => ({
        kind: "missing" as const,
        min: Number(row.range_min),
        max: Number(row.range_max),
      })),
      ...duplicateRes.getRowObjects().map((row) => ({
        kind: "duplicate" as const,
        min: Number(row.range_min),
        max: Number(row.range_max),
      })),
    ].toSorted((a, b) => a.min - b.min || (a.kind === "missing" ? -1 : 1));

    if (issues.length === 0) {
      console.log("\nNo missing or duplicate timepoint ranges found.");
      return;
    }

    console.log(`\nFound ${issues.length} erroneous range(s):`);
    for (const issue of issues) {
      console.log(`${issue.kind} ${formatRange(issue.min, issue.max)}`);
    }
  } finally {
    connection.closeSync();
  }
}

async function main(streamPath: string) {
  if (!streamPath) {
    throw new Error("No stream provided. Usage: bun lakehouse/findTimepointGaps.ts <stream>");
  }
  if (!streams.includes(streamPath)) {
    throw new Error(`Invalid stream. Options: ${streams.join(", ")}`);
  }

  await findMissingRanges(streamPath);
}

await main(process.argv[2]);