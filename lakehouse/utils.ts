import { DuckDBConnection, type DuckDBListValue, type DuckDBResultReader } from "@duckdb/node-api";
import { basename } from "node:path";

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
      const label = statement
        .replaceAll(/(--.*)?\n+\s*/g, " ")
        .trim()
        .slice(0, 40);
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

export type FileConfig = {
  format: string;
  compression: string;
  extension: string;
  description: string;
  split: boolean;
  sample: boolean;
  single: boolean;
  contentType?: string;
  contentEncoding?: string;
};
export async function uploadLocalFiles(
  filesRes: DuckDBResultReader,
  file: FileConfig,
  prefix: string,
  metadata: Record<string, any> = {},
  bucket: string,
) {
  const outputs = [];
  for (const outputFileBatch of filesRes.getRowObjects()) {
    const actualOutputNames = [];
    let totalSize = 0;
    for (const outputFile of (outputFileBatch.Files as DuckDBListValue).items as string[]) {
      const localFile = Bun.file(outputFile);
      const actualFilename = basename(outputFile);
      console.log("Uploading", actualFilename, "to S3", bucket);
      await Bun.s3.write(prefix + actualFilename, localFile, {
        bucket,
        type: file.contentType ?? localFile.type,
        contentEncoding: file.contentEncoding,
        contentDisposition: `attachment; filename="${actualFilename}"`,
      });
      console.log(actualFilename, "File size:", localFile.size.toLocaleString(), "B");
      totalSize += localFile.size;
      actualOutputNames.push(prefix + actualFilename);
    }
    outputs.push({
      files: actualOutputNames,
      count: Number(outputFileBatch.Count),
      totalSizeBytes: totalSize,
      format: file.format,
      extension: file.extension,
      compression: file.compression,
      description: file.description,
      ...metadata,
    });
  }
  return outputs;
}
