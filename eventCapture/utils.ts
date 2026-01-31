import {open, stat} from 'fs/promises';
import {basename} from "node:path";
import {S3Client} from "bun";
import {createReadStream, existsSync} from "node:fs";
import {get, RequestOptions} from "https";
import {readdir} from "node:fs/promises";
import {pipeline} from "node:stream/promises";
import {createGzip} from "node:zlib";

export async function getLastJsonLine(filePath: string): Promise<Record<string, any> | undefined> {
    if (!existsSync(filePath)) return undefined;
    const stats = await stat(filePath);
    const size = stats.size;
    if (size === 0) return undefined;
    const file = await open(filePath, 'r');
    try {
        const buffer = Buffer.alloc(1);
        let pos = size;
        while (true) {
            let char: string;
            do {
                if (pos <= 0) {
                    return undefined;
                }
                pos--;
                await file.read(buffer, 0, 1, pos);
                char = buffer.toString('utf8', 0, 1);
            } while (char === '\n' || char === '\r');
            let start = pos;
            while (start > 0) {
                start--;
                await file.read(buffer, 0, 1, start);
                char = buffer.toString('utf8', 0, 1);
                if (char === '\n' || char === '\r') {
                    start++;
                    break;
                }
            }
            const length = pos - start + 1;
            const lineBuffer = Buffer.alloc(length);
            await file.read(lineBuffer, 0, length, start);
            const line = lineBuffer.toString('utf8').trim();
            if (line !== '') {
                try {
                    const parsed = JSON.parse(line);
                    if(parsed.error){
                        pos = start - 1;
                        continue;
                    }else{
                        return parsed;
                    }
                } catch {
                    pos = start - 1;
                    continue;
                }
            }
            pos = start - 1;
        }
    } finally {
        await file.close();
    }
}

export async function writeStreamToFile(stream: AsyncIterable<Buffer>, filename: string) {
    const sink = Bun.file(filename)
    const writer = sink.writer()
    let bytesWritten = 0;
    try {
        // TODO: could this just be await pipeline(stream, createWriteStream(filename))?
        //  Might be simpler and equal performance.
        for await(const chunk of stream) {
            if (chunk.length === 1 && chunk[0] === 0x0a) {
                continue //heartbeat received
            }
            bytesWritten += await writer.write(chunk)
        }
    } catch (e) {
        console.error('Error writing stream to file', filename, e)
    } finally {
        bytesWritten += await writer.flush()
    }

    bytesWritten += await writer.end()
    if (bytesWritten === 0) {
        console.log('No data written to file', filename)
        await sink.delete()
        return false
    } else {
        console.log('Wrote', bytesWritten, 'bytes to file', filename)
        return true
    }
}


const {S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, SINK_BUCKET} = process.env
console.log('S3 Bucket:', SINK_BUCKET,)
const client = new S3Client({
    accessKeyId: S3_ACCESS_KEY_ID,
    secretAccessKey: S3_SECRET_ACCESS_KEY,
    bucket: SINK_BUCKET,
    endpoint: S3_ENDPOINT,
    region: S3_REGION
})

export async function uploadToS3(file: string, streamName: string) {
    console.log(new Date(), 'Uploading', file, 'to S3')
    const objectPath = getS3ObjectPath(file,streamName)
    const s3file = client.file(objectPath);
    const writer = s3file.writer()
    const bytesWritten = await pipeline(createReadStream(file), createGzip(), async function (source) {
        let bytesWriten = 0;
        for await (const chunk of source) {
            bytesWriten += await writer.write(chunk)
        }
        bytesWriten += await writer.end()
        return bytesWriten
    })
    console.log(new Date(), 'Uploaded', bytesWritten, 'bytes to S3', objectPath)
}

export async function streamFromCh(streamPath: string, startFromTimepoint?: number) {
    const auth = process.env.STREAM_KEY + ":"
    const path = "/" + streamPath + (typeof startFromTimepoint === "number" ? `?timepoint=${startFromTimepoint}` : "")
    const options: RequestOptions = {hostname: "stream.companieshouse.gov.uk", path, auth}
    const responseStream = new Promise<AsyncIterable<Buffer>>((resolve, reject) => get(options, (res) => {
        if (res.statusCode === 200) {
            console.log(new Date(),'Connected to stream', streamPath, )
            setTimeout(() => res.destroy(new Error('self-terminated connection after some time')),60_000)
            resolve(res)
        } else reject(new Error(`Failed to connect to stream: ${res.statusCode}`))
    }).end())
    return responseStream
}

export async function getLastSavedTimepoint(outputDir: string) {
    const files = await readdir(outputDir)
    // Filter for .json files and sort by timestamp (filename is the timestamp)
    const jsonFiles = files
        .filter(f => f.endsWith('.json'))
        .sort()

    if (jsonFiles.length === 0) {
        return undefined
    }

    const lastFile = `${outputDir}/${jsonFiles.at(-1)}`
    const lastEvent = await getLastJsonLine(lastFile)
    console.log('Last event', lastEvent?.event)
    const timepoint = lastEvent?.event.timepoint;
    if (timepoint) {
        console.log('Picking up from timepoint', timepoint)
    } else {
        console.log('No timepoint found. Starting from scratch.')
    }
    return timepoint
}

export async function cleanupOldFiles(outputDir: string, streamName:string){
    const files = await readdir(outputDir)
    const jsonFiles = files
        .filter(f => f.endsWith('.json'))
        .sort()
        .slice(0,-1) // never delete the last file
    for(const file of jsonFiles){
        const filePath = `${outputDir}/${file}`
        const stats = await stat(filePath)
        const fileAge = Date.now() - stats.mtimeMs
        // keep files for at least 2 days to allow picking up from prior timepoint
        const TWO_DAYS = 1000 * 60 * 60 * 24 * 2
        if(fileAge > TWO_DAYS){
            const objectPath = getS3ObjectPath(filePath,streamName)
            const s3file = client.file(objectPath);
            const uploaded = await s3file.exists()
            if(uploaded) {
                console.log(new Date(), 'Deleting old file', filePath)
                await Bun.file(filePath).delete()
            }else{
                console.warn(new Date(), 'Old file not uploaded to S3', filePath)
            }
        }
    }
}

export async function uploadExistingFilesToS3(outputDir: string, streamName:string){
    const files = await readdir(outputDir)
    const jsonFiles = files
        .filter(f => f.endsWith('.json'))
        .sort()
    for(const file of jsonFiles) {
        const filePath = `${outputDir}/${file}`
        const objectPath = getS3ObjectPath(filePath,streamName)
        const s3file = client.file(objectPath);
        const uploaded = await s3file.exists()
        if(!uploaded) {
            console.log(new Date(),'Uploading local file to S3', filePath)
            await uploadToS3(filePath,streamName)
        }
    }
}

function getS3ObjectPath(filename:string,streamName:string){
    return `${streamName}/${basename(filename)}.gz`
}