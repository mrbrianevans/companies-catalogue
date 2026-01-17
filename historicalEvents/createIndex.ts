// this creates a duckdb index of what timepoint range each file contains

import { DuckDBInstance } from '@duckdb/node-api';
const db = await DuckDBInstance.create('./index.db');

const connection = await db.connect();
//TODO: this needs to work for all streams, perhaps in the same table.
// Would be good if a single command could build it for the entire bucket.
// Would be good to update only the ones that don't exist in the index.
console.time('create index')
await connection.run(`
CREATE OR REPLACE TABLE files AS (
    SELECT filename, 
    MIN(event.timepoint) as min, 
    MAX(event.timepoint) as max,
    SPLIT(filename, '/')[4] AS stream
    FROM 's3://companies-stream-sink/charges/*.json.gz' 
    GROUP BY filename
);
`)
console.timeEnd('create index')

connection.closeSync();
db.closeSync();
