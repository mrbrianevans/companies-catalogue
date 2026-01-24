import { DuckDBInstance, INTEGER, VARCHAR } from '@duckdb/node-api';
import {connection} from "./duckdbConnection.js";

// This file retrieves data from the index of files,
// which stores the min and max timepoint of each file in S3.


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

    const bigIntRange = output.getRowObjects()[0] as {stream_min:bigint, stream_max: bigint}
    const {stream_min:min, stream_max: max} = bigIntRange

    if(!min || !max) {
        console.warn('No data indexed for ', stream)
        return null
    }

    const range = {min: Number(min), max: Number(max)};

    console.log('Min max range for', stream, range)
    return range
}