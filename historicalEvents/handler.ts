import {PassThrough, Readable, Writable} from 'node:stream'
import {fileSequence} from "./fileSequence";
import {s3} from 'bun'
import {createGunzip} from 'node:zlib'
import { pipeline } from "node:stream/promises";
import split2 from 'split2';

export async function getHistoricalStream(path: string, timepoint: number): Promise<Readable>{

    // get starting file and sequence of following files (using duckdb)
    const files = await fileSequence(path, timepoint)
    // seek in starting file to first timepoint requested
    // stream the rest of files with gunzip but no json parse

    const stream = new PassThrough();
    streamFilesToPassThrough(stream, files, timepoint).then(()=>stream.end())
    return stream;
}

export function seekInFile(filename: string, timepoint: number): Readable {
    return streamWholeFile(filename)
        .pipe(split2(JSON.parse))
       .filter(event => event.event.timepoint >= timepoint)
       .map(event => Buffer.from(JSON.stringify(event)))
}

function streamWholeFile(fullS3Path: string): Readable{
    console.log('streaming', fullS3Path)
    const filepath = new URL(fullS3Path).pathname.slice(1);
    const fileStream = s3.file(filepath).stream()
    const unzipper =  createGunzip()
    return Readable.from(fileStream).pipe(unzipper)
}

async function streamFilesToPassThrough(outputStream: Writable, files: string[], timepoint: number){
    const [firstFile, ...restFiles] = files;
    {
        const fileStream = seekInFile(firstFile, timepoint)
        await pipeline(fileStream, outputStream)
    }

    for(const file of restFiles){
        const fileStream = streamWholeFile(file)
        await pipeline(fileStream, outputStream)
    }
}