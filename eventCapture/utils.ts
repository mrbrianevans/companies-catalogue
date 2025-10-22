import {mkdir, open, stat} from 'fs/promises';
import {dirname} from "node:path";
import {S3Client} from "bun";
import {existsSync} from "node:fs";
import {get, RequestOptions} from "https";
import {setTimeout} from "node:timers/promises";
import { readdir } from "node:fs/promises";

export async function getLastJsonLine(filePath: string): Promise<Record<string, any> | undefined> {
    if(!existsSync(filePath)) return undefined;
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
                    return JSON.parse(line);
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
    const outDir = dirname(filename)
    await mkdir(outDir, {recursive: true})
    const sink = Bun.file(filename)
    const writer = sink.writer()
    let bytesWritten = 0;
    try {
        for await(const chunk of stream) {
            if(chunk.length === 1 && chunk[0] === 0x0a) {
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
    if(bytesWritten === 0) {
        console.log('No data written to file', filename)
        await sink.delete()
        return false
    }else {
        console.log('Wrote', bytesWritten, 'bytes to file', filename)
        return true
    }
}


const { S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, SINK_BUCKET } = process.env
const client = new S3Client({
    accessKeyId: S3_ACCESS_KEY_ID,
    secretAccessKey: S3_SECRET_ACCESS_KEY,
    bucket: SINK_BUCKET,
    endpoint: S3_ENDPOINT,
    region: S3_REGION
})

export async function uploadToS3(file: string) {
    const s3file = client.file(file);
    const localFile = Bun.file(file);
    await s3file.write(localFile, {type: 'application/json'});
}

export async function streamFromCh(streamPath: string, startFromTimepoint?: number) {
    const auth = process.env.STREAM_KEY + ":"
    const path = "/" + streamPath + (typeof startFromTimepoint === "number" ? `?timepoint=${startFromTimepoint}` : "")
    const options: RequestOptions = {hostname: "stream.companieshouse.gov.uk", path, auth}
    const responseStream = new Promise<AsyncIterable<Buffer>>((resolve, reject) => get(options, (res) => {
        if (res.statusCode === 200) {
            console.log('Connected to stream', streamPath, new Date())
            // setTimeout(10000).then(() => res.destroy(new Error('test timeout')))
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
        .sort((a, b) => {
            const timestampA = parseInt(a.replace('.json', ''))
            const timestampB = parseInt(b.replace('.json', ''))
            return timestampB - timestampA // Descending order
        })

    if (jsonFiles.length === 0) {
        return undefined
    }

    const lastFile = `${outputDir}/${jsonFiles[0]}`
    const lastEvent = await getLastJsonLine(lastFile)
    const timepoint = lastEvent?.event.timepoint;
    if(timepoint){
        console.log('Picking up from timepoint', timepoint)
    }else{
        console.log('No timepoint found. Starting from scratch.')
    }
    return timepoint
}