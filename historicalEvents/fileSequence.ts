import {DuckDBInstance, INTEGER, VARCHAR} from '@duckdb/node-api';
const db = await DuckDBInstance.create('./index.db');

const connection = await db.connect();


export async function fileSequence(stream: string, timepoint: number) {
console.log({stream, timepoint})
    const output = await connection.runAndReadAll(`
        SELECT *
        FROM files
        WHERE max >= $timepoint
          AND stream = $stream
        ORDER BY min;
    `, {
        timepoint,
        stream
    }, {timepoint: INTEGER, stream: VARCHAR})

    const matchingFiles = output.getRowObjects()
    console.log('matching files', matchingFiles)
    return matchingFiles.map(f=>f.filename as string)
}