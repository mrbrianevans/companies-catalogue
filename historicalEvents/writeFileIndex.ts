// Writes a duckdb index of what timepoint range each file contains
// Can create or update an existing index.db file.
// Run this daily.

import {ARRAY, arrayValue, DuckDBConnection, DuckDBInstance, VARCHAR} from '@duckdb/node-api';

const writeIndex = async () => {
    const db = await DuckDBInstance.create('./index.db');

    const connection = await db.connect();

    console.log('Building latest file index of min-max timepoints in each S3 file of events')

    await connection.run(`CREATE OR REPLACE SECRET secret (
      TYPE s3,
      KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
      SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
      ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? '').host}',
      REGION '${process.env.S3_REGION}'
    );`)

    console.time('create index if not exists')
    await connection.run(`
CREATE TABLE IF NOT EXISTS files AS (
    SELECT filename, 
    MIN(event.timepoint) as min, 
    MAX(event.timepoint) as max,
    SPLIT(filename, '/')[4] AS stream
    --TODO: investigate parsing errors in officers data
    FROM read_json('s3://companies-stream-sink/*/*.json.gz', ignore_errors = true) 
    GROUP BY filename
);
`)
    console.timeEnd('create index if not exists')

// I think this actually requires first querying which files are missing, then a second query filtered only on those files.otherwise it rebuilds all of it.
    console.time('update index')
// add any missing ones. seems to be rebuilding from scratch before filtering out ones already done.
    await connection.run(`
    INSERT INTO files (
        SELECT filename,
        MIN(event.timepoint) as min,
        MAX(event.timepoint) as max,
        SPLIT(filename, '/')[4] AS stream
        FROM read_json('s3://companies-stream-sink/*/*.json.gz', ignore_errors = true)
        WHERE filename NOT IN (SELECT filename FROM files)
        GROUP BY filename
    );
`)
    console.timeEnd('update index')

    connection.closeSync();
    db.closeSync();
}


export async function updateIndex(dbConn: DuckDBConnection){
    console.log(new Date(), 'Updating index')
    // find files in S3 that aren't indexed
    const res = await dbConn.runAndReadAll(`
    SELECT file 
    FROM glob('s3://companies-stream-sink/*/*.json.gz')
    WHERE file NOT IN (SELECT filename FROM files)
    LIMIT 20;
    `)

    const files = res.getRowObjects().map(f=>f.file as string)

    if(files.length) {
        console.log('Inserting files into index', files)

        console.time('update index')
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
`,)
        console.timeEnd('update index')
    }else{
        console.log('No new files to index')
        const res = await dbConn.runAndReadAll(`
            SELECT stream, COUNT(*) as file_count 
            FROM files
            GROUP BY stream
            ;
    `)
        console.log('Index contains', res.getRowObjects().map(f=>`\n${f.stream}: ${f.file_count}`))
    }
}
