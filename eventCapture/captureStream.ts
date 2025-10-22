import {setTimeout} from 'node:timers/promises'
import {getLastSavedTimepoint, streamFromCh, uploadToS3, writeStreamToFile} from "./utils";

/*

Connect to stream, pipe to a local json file sink.
Upload sink files to S3 bucket.
Re-connect.

*/

const streamName = process.argv[2]
if (!streamName) throw new Error('No stream name provided. Usage bun captureStream [officers]')

const outputDir = `output/${streamName}`

// Track process exits
process.on('SIGINT', () => {
    console.log('Exiting due to SIGINT');
    process.exit(0);
});
process.on('SIGTERM', () => {
    console.log('Exiting due to SIGTERM');
    process.exit(0);
});
process.on('SIGHUP', () => {
    console.log('Exiting due to SIGHUP');
    process.exit(0);
});
process.on('exit', () => console.log('Exiting process'));


while (true) {
    try {
        const lastTimepoint = await getLastSavedTimepoint(outputDir)
        const incomingStream = await streamFromCh(streamName, lastTimepoint + 1)
        const outputName = `${outputDir}/${Date.now()}.json`
        const written = await writeStreamToFile(incomingStream, outputName)
        if(written) await uploadToS3(outputName)
    } catch (error) {
        console.error('Error capturing stream', streamName, error)
    } finally {
        console.log('Pausing before re-connecting to stream')
        await setTimeout(65000)
    }
}

