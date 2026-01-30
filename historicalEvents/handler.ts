import { PassThrough, Readable, Writable } from 'node:stream'
import {getFileSequence, getMinMaxRange} from "./readFileIndex.js";
import { s3 } from 'bun'
import { createGunzip } from 'node:zlib'
import { pipeline } from "node:stream/promises";
import { compose } from "node:stream";
import split2 from 'split2';
import {makeError, streams} from "./utils.js";

export async function handleStreamRequest(path:string, timepointInputString:string|null, abortSignal: AbortSignal):Promise<Response>{
    console.log(new Date(), 'Handling stream request for', path, timepointInputString)
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
        const headers = new Headers({
            // to exactly match companies house headers
            'access-control-allow-credentials': 'true',
            'access-control-allow-headers': 'origin, content-type, content-length, user-agent, host, accept, authorization',
            'access-control-expose-headers': 'Location, www-authenticate, cache-control, pragma, content-type, expires, last-modified',
            'access-control-max-age': '3600',
            'content-type': 'text/plain; charset=utf-8'
        })
        // @ts-ignore - should be using Bun's response type
        return new Response(outputStream, {headers})
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
    stream.setMaxListeners(2*files.length + 3)
    streamFilesToPassThrough(stream, files, timepoint,abortSignal)
        .then(()=> {
            console.log(new Date(), 'Finished streaming peacefully')
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
            .pipe(split2())
            .filter(line => {
                // use regex to find timepoint. quicker than JSON parse (less cpu usage)
                if(!line) return false
                const match = line.match(/"timepoint":\s*(\d+)/);
                if(!match) return false;
                const eventTimepoint =  Number(match[1]) ;
                return eventTimepoint >= timepoint
            })
            .map(event => Buffer.from((event)+'\n'))
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