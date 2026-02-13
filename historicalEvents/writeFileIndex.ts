// Writes a duckdb index of what timepoint range each file contains
// Can create or update an existing index.db file.
// Run this daily.

import { DuckDBConnection } from "@duckdb/node-api";

export async function updateIndex(dbConn: DuckDBConnection) {
  console.log(new Date(), "Updating index");
  // find files in S3 that aren't indexed
  const res = await dbConn.runAndReadAll(`
    SELECT file 
    FROM glob('s3://companies-stream-sink/*/*.json.gz')
    WHERE file NOT IN (SELECT filename FROM files)
    LIMIT 20;
    `);

  const files = res.getRowObjects().map((f) => f.file as string);

  if (files.length) {
    console.log("Inserting files into index", files);

    console.time("update index");
    // add any missing ones. seems to be rebuilding from scratch before filtering out ones already done.
    await dbConn.run(`
    INSERT INTO files (
        SELECT filename,
            MIN(event.timepoint) as min,
            MAX(event.timepoint) as max,
            SPLIT(filename, '/')[4] AS stream,
--             COUNT(*) AS count,
--             MIN(event.published_at) as min_published_at,
--             MAX(event.published_at) as max_published_at
        FROM read_json(${JSON.stringify(files)}, ignore_errors = true)
        WHERE filename NOT IN (SELECT filename FROM files)
        GROUP BY filename
    );
`);
    console.timeEnd("update index");
  } else {
    console.log("No new files to index");
    const res = await dbConn.runAndReadAll(`
            SELECT stream, COUNT(*) as file_count 
            FROM files
            GROUP BY stream
            ;
    `);
    console.log(
      "Index contains",
      res.getRowObjects().map((f) => `\n${f.stream}: ${f.file_count}`),
    );
  }
}
