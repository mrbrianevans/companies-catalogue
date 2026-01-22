import { PassThrough, Readable, Writable } from 'node:stream'
import {getFileSequence, getMinMaxRange} from "./readFileIndex.js";
import { s3 } from 'bun'
import { createGunzip } from 'node:zlib'
import { pipeline } from "node:stream/promises";
import { compose } from "node:stream";
import split2 from 'split2';
import {makeError, streams} from "./utils.js";

export async function handleStreamRequest(path:string, timepointInputString:string|null, abortSignal: AbortSignal):Promise<Response>{
    try {

        // Request validation
        if (!streams.includes(path))
            return makeError(400, 'Invalid stream. Options: ' + streams.join(', '))
        if (!timepointInputString)
            return makeError(400, 'Missing timepoint parameter')
        const timepoint = Number(timepointInputString)
        if (isNaN(timepoint))
            return makeError(400, 'Invalid timepoint parameter')

        const validRange = await getMinMaxRange(path)
        if (!validRange)
            return makeError(501, 'No data available for ' + path)

        if (timepoint > validRange.max)
            return makeError(416, 'Timepoint out range (too big)')
        if (timepoint < validRange.min)
            return makeError(416, 'Timepoint out range (too small)')

        const outputStream = await getHistoricalStream(path, timepoint, abortSignal)
        // @ts-ignore - should be using Bun's response type
        return new Response(outputStream)
    }catch(error){
        console.error('Internal error',error)
        return makeError(500, 'Internal server error')
    }
}

export async function getHistoricalStream(path: string, timepoint: number, abortSignal: AbortSignal): Promise<Readable>{
    const files = await getFileSequence(path, timepoint)

    const firstPath = new URL(files[0]).pathname.slice(1);
    const firstFileExists = files[0] && await s3.exists(firstPath);
    if(!firstFileExists) throw new Error('Indexed file not accessible in S3 '+files[0]);

    const stream = new PassThrough();
    stream.setMaxListeners(2*files.length + 1)
    streamFilesToPassThrough(stream, files, timepoint,abortSignal)
        .then(()=> {
            console.log('Finished streaming peacefully')
            stream.end()
        })
        .catch((reason)=> {
            console.warn('Errored while streaming', reason)
            stream.end(JSON.stringify({error: 'Error while streaming'}))
        })
    return stream;
}

function seekInFile(filename: string, timepoint: number): Readable {
    return streamWholeFile(filename).on('error', (err) => {console.log('whole file error', err)})
        .pipe(split2(JSON.parse))
        .filter(event => event.event.timepoint >= timepoint)
        .map(event => Buffer.from(JSON.stringify(event)+'\n'))
}

function streamWholeFile(fullS3Path: string): Readable {
    console.log('Streaming file', fullS3Path)
    const filepath = new URL(fullS3Path).pathname.slice(1);

    const fileStream = s3.file(filepath).stream()
    const unzipper = createGunzip()
    return compose(fileStream, unzipper)
}

async function streamFilesToPassThrough(outputStream: Writable, files: string[], timepoint: number, abortSignal: AbortSignal){
    const [firstFile, ...restFiles] = files;
    {
        const fileStream = seekInFile(firstFile, timepoint)
        await pipeline(fileStream, outputStream, {end: false, signal: abortSignal})
    }

    for(const file of restFiles){
        const fileStream = streamWholeFile(file)
        await pipeline(fileStream, outputStream, {end: false,signal: abortSignal})
    }
}