// Writes a duckdb index of what timepoint range each file contains
// Can create or update an existing index.db file.
// Run this daily.

import { DuckDBInstance } from '@duckdb/node-api';
const db = await DuckDBInstance.create('./index.db');

const connection = await db.connect();

console.time('create index if not exists')
await connection.run(`
CREATE TABLE IF NOT EXISTS files AS (
    SELECT filename, 
    MIN(event.timepoint) as min, 
    MAX(event.timepoint) as max,
    SPLIT(filename, '/')[4] AS stream
    FROM 's3://companies-stream-sink/*/*.json.gz' 
    GROUP BY filename
);
`)
console.timeEnd('create index if not exists')

console.time('update index')
// add any missing ones
await connection.run(`
    INSERT INTO files (
        SELECT filename, 
        MIN(event.timepoint) as min, 
        MAX(event.timepoint) as max,
        SPLIT(filename, '/')[4] AS stream
        FROM 's3://companies-stream-sink/*/*.json.gz'
        WHERE filename NOT IN (SELECT filename FROM files)
        GROUP BY filename
    );
`)
console.timeEnd('update index')

connection.closeSync();
db.closeSync();
