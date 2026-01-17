import {DuckDBInstance, INTEGER, VARCHAR} from '@duckdb/node-api';

const db = await DuckDBInstance.create('./index.db');
const connection = await db.connect();


export async function getFileSequence(stream: string, timepoint: number) {
    console.log('Get file sequence for',{stream, timepoint})
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
    console.log('Matching files', matchingFiles)
    return matchingFiles.map(f=>f.filename as string)
}

export async function getMinMaxRange(stream: string){
    const output = await connection.runAndReadAll(`
        SELECT 
            MIN(files.min) as stream_min, 
            MAX(files.max) as stream_max
        FROM files
        WHERE stream = $stream;
    `, {
        stream
    }, { stream: VARCHAR})

    const range = output.getRowObjects()[0] as {stream_min:number, stream_max: number}
    const {stream_min:min, stream_max: max} = range

    if(!min || !max) {
        console.warn('No data indexed for ', stream)
        return null
    }

    console.log('Min max range for', stream, {min, max})
    return {min, max}
}