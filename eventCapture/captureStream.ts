import {
  cleanupOldFiles,
  getLastSavedTimepoint,
  streamFromCh,
  uploadToS3,
  writeStreamToFile,
} from "./utils";
import { randomUUIDv7 } from "bun";
import { mkdir } from "fs/promises";

/*
Connect to stream, pipe to a local json file sink.
Upload sink files to S3 bucket.
*/

const streamName = process.argv[2];
if (!streamName) throw new Error("No stream name provided. Usage bun captureStream [officers]");

const outputDir = `output/${streamName}`;
await mkdir(outputDir, { recursive: true });
console.log(new Date(), "Output directory", outputDir);

async function captureStream(streamName:string) {
    const lastTimepoint = await getLastSavedTimepoint(outputDir, streamName);
    const pickUpFrom = lastTimepoint ? lastTimepoint + 1 : undefined;
    const incomingStream = await streamFromCh(streamName, pickUpFrom);
    const outputName = `${outputDir}/${randomUUIDv7()}.json`;
    await writeStreamToFile(incomingStream, outputName);
    await uploadToS3(outputName, streamName);
    await cleanupOldFiles(outputDir, streamName);
}

await captureStream(streamName);
