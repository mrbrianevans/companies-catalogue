import { PassThrough, Readable, Writable } from 'node:stream'
import { getFileSequence } from "./readFileIndex.js";
import { s3 } from 'bun'
import { createGunzip } from 'node:zlib'
import { pipeline } from "node:stream/promises";
import { compose } from "node:stream";
import split2 from 'split2';

export async function getHistoricalStream(path: string, timepoint: number): Promise<Readable>{
    const files = await getFileSequence(path, timepoint)
    const firstFileExists = files[0] && await s3.exists(files[0]);
    if(!firstFileExists) throw new Error('Indexed file not accessible in S3 '+files[0]);

    const stream = new PassThrough();
    streamFilesToPassThrough(stream, files, timepoint)
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
    return streamWholeFile(filename).on('error', (err) => {console.log('whole file rror', err)})
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