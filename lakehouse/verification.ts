// Tests to verify the integrity and quality of the data

import { setupLakehouseConnection } from "./connection.js";
import { streams } from "./utils.js";
const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

// only start testing from this point onwards since there was a data issue before hand
const startingTimepoints: Record<string, number> = {
  officers: 36300854,
};

async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }
  const startingTimepoint = startingTimepoints[streamPath] ?? 0;
  console.log("Testing data-verification", streamPath, "from timepoint", startingTimepoint);
  const { connection } = await setupLakehouseConnection();
  await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

  const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`);
  const tables = tablesRes.getRowObjects().map((r) => r.name as string);
  console.log("tables in lakehouse", tables);

  // check lakehouse events timepoints
  const lakehouseTimepointRes = await connection.runAndReadAll(`
        SELECT COUNT(*) as count,
               COUNT(DISTINCT event.timepoint) as distinct_timepoints,
               MAX(event.timepoint) - MIN(event.timepoint) + 1 as diff_timepoints
        FROM events
        WHERE event.timepoint >= ${startingTimepoint};
    `);

  const lakehouseTimepoints = lakehouseTimepointRes.getRowObjects()[0];
  console.log(
    "Row count",
    lakehouseTimepoints.count,
    "Distinct timepoints",
    lakehouseTimepoints.distinct_timepoints,
    "Timepoint range",
    lakehouseTimepoints.diff_timepoints,
  );
  if (lakehouseTimepoints.diff_timepoints !== lakehouseTimepoints.distinct_timepoints) {
    throw new Error(
      "Timepoints in lakehouse are not contiguous. Min-max Diff does not match distinct timepoints.",
    );
  }
  if (lakehouseTimepoints.count !== lakehouseTimepoints.distinct_timepoints) {
    throw new Error(
      "Timepoints in lakehouse are not contiguous. Count does not match distinct timepoints",
    );
  }

  // check that events are being regularly added
  const lakehousePublishedAtRes = await connection.runAndReadAll(`
        SELECT MIN(event.published_at) as min_published_at, 
               MAX(event.published_at) as max_published_at
        FROM events
        WHERE event.timepoint >= ${startingTimepoint};
    `);
  const lakehousePublishedAtRow = lakehousePublishedAtRes.getRowObjects()[0];
  const latestEvent = new Date(String(lakehousePublishedAtRow.max_published_at));
  const now = new Date();
  console.log("Latest event published at", latestEvent.toISOString());
  const age = now.getTime() - latestEvent.getTime();
  if (age > 1000 * 60 * 60 * 48) {
    throw new Error("Latest event published more than 48 hours ago");
  }

  console.log("All tests passed.");
  connection.closeSync();
}

await main(process.argv[2]);
