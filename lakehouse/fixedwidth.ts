// Load the latest prod216 fixedwidth snapshot (companies and persons) from the sink into the lakehouse.
// Each file is a complete snapshot, so the lakehouse table is replaced rather than appended to.

import { DuckDBConnection } from "@duckdb/node-api";
import { executeSql } from "./utils.js";

import lakehouseSetupFixedwidthSql from "./lakehouse_setup_fixedwidth.sql" with { type: "text" };
import lakehouseFixedwidthCompaniesSql from "./lakehouse_fixedwidth_companies.sql" with { type: "text" };
import lakehouseFixedwidthPersonsSql from "./lakehouse_fixedwidth_persons.sql" with { type: "text" };

const kinds = {
  companies: lakehouseFixedwidthCompaniesSql,
  persons: lakehouseFixedwidthPersonsSql,
};

/**
 * @param connection - lakehouse connection, with the fixedwidth schema created
 * @param basePath - directory containing the companies_YYYY-MM-DD.parquet and persons_YYYY-MM-DD.parquet files
 */
export async function loadFixedwidth(connection: DuckDBConnection, basePath: string) {
  console.time("setup lakehouse");
  await executeSql(connection, lakehouseSetupFixedwidthSql);
  console.timeEnd("setup lakehouse");

  for (const [kind, lakehouseSql] of Object.entries(kinds)) {
    const searchPath = `${basePath}/${kind}_*.parquet`;
    await connection.run(`SET VARIABLE searchPath = '${searchPath}';`);
    const latest = await connection.runAndReadAll(`SELECT file FROM
    (SELECT max(file) AS file FROM glob(getvariable('searchPath')))
    WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files)`);
    const file = latest.getRowObjects()[0]?.file as string | null | undefined;
    if (!file) {
      console.log("No new", kind, "fixedwidth file to load from", searchPath);
      continue;
    }
    console.log("Loading", file, "into lakehouse");
    await connection.run(`SET VARIABLE file = '${file}';`);
    console.time(`load ${kind}`);
    await executeSql(connection, lakehouseSql);
    console.timeEnd(`load ${kind}`);
  }
}
