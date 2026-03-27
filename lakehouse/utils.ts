import { DuckDBConnection } from "@duckdb/node-api";

export const streams = [
  "companies",
  "filings",
  "officers",
  "persons-with-significant-control",
  "charges",
  "insolvency-cases",
  "disqualified-officers",
  "company-exemptions",
  "persons-with-significant-control-statements",
];
export const makeError = (code: number, message: string) =>
  Response.json({ error: message }, { status: code });

/**
 * Designed for executing a collection of statements defined in a .sql file.
 * Executes a given SQL string on a DuckDB connection, one statement at a time.
 * Logs execution time and outputs the results or errors for each statement.
 *
 * @param {DuckDBConnection} duckdbConnection - The active DuckDB connection to execute the SQL statements on.
 * @param {string} sql - A string containing one or more SQL statements separated by semicolons.
 */
export async function executeSql(duckdbConnection: DuckDBConnection, sql: string): Promise<void> {
  const statements = sql.split(";");
  for (const statement of statements) {
    if (!statement.trim()) continue;
    try {
      const label = statement.trim().split("\n")[0].trim().slice(0, 40);
      console.time("Execute SQL statement: " + label);
      const res = await duckdbConnection.runAndReadAll(statement + ";");
      console.timeEnd("Execute SQL statement: " + label);
      if (res.rowsChanged) console.log("Rows changed:", res.rowsChanged);
      if (res.getRowObjects().length > 0) {
        console.log("Result:", res.getRowObjects());
      }
    } catch (e) {
      console.error("Error executing SQL statement:", statement, "Error:", e);
      throw e;
    }
  }
}
