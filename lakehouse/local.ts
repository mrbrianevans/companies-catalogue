// For local edits to the lakehouse. Run with care.

import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";
const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, "_");

async function main() {
  const { connection, tempDbFile, remoteCataloguePath } = await setupLakehouseConnection();

  const streamPath = "persons-with-significant-control-statements";
  await connection.run(`USE lakehouse.${getSchema(streamPath)};`);

  const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`);
  const tables = tablesRes.getRowObjects().map((r) => r.name as string);
  console.log("tables in lakehouse", tables);

  const res = await connection.runAndReadAll(`
    SELECT COUNT(*) as count, COUNT(DISTINCT event.timepoint) as distinct_timepoints
    FROM snapshot;
    `);

  const row = res.getRowObjects()[0];
  console.log("Row count", row.count, "distinct timepoints", row.distinct_timepoints);

  if (process.argv.includes("--save")) {
    await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath });
  }
}

await main();
