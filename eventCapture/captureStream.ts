import {scheduler} from 'node:timers/promises'
import {
    cleanupOldFiles,
    getLastSavedTimepoint,
    streamFromCh,
    uploadExistingFilesToS3,
    uploadToS3,
    writeStreamToFile
} from "./utils";
import { randomUUIDv7 } from "bun";

/*

Connect to stream, pipe to a local json file sink.
Upload sink files to S3 bucket.
Re-connect.

*/

const streamName = process.argv[2]
if (!streamName) throw new Error('No stream name provided. Usage bun captureStream [officers]')

const outputDir = `output/${streamName}`
console.log(new Date(),'Output directory', outputDir)

// Track process exits
process.on('SIGINT', () => {
    console.log(new Date(),'Exiting due to SIGINT');
    process.exit(0);
});
process.on('SIGTERM', () => {
    console.log(new Date(),'Exiting due to SIGTERM');
    process.exit(0);
});
process.on('SIGHUP', () => {
    console.log(new Date(),'Exiting due to SIGHUP');
    process.exit(0);
});
process.on('exit', () => console.log(new Date(),'Exiting process'));


await uploadExistingFilesToS3(outputDir, streamName)


while (true) {
    try {
        const lastTimepoint = await getLastSavedTimepoint(outputDir)
        const incomingStream = await streamFromCh(streamName, lastTimepoint + 1)
        const outputName = `${outputDir}/${randomUUIDv7()}.json`
        const written = await writeStreamToFile(incomingStream, outputName)
        if(written) await uploadToS3(outputName,streamName)
        await cleanupOldFiles(outputDir,streamName)
    } catch (error) {
        console.error(new Date(),'Error capturing stream', streamName, error)
    } finally {
        console.log(new Date(), 'Pausing before re-connecting to stream')
        await scheduler.wait(65000)
    }
}

