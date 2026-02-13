import split2 from "split2";
import { Readable } from "node:stream";

const limit = 1_000_000;
const stream = process.argv[2] || "persons-with-significant-control-statements";
console.log("Testing", stream, "stream");
const timepointRes = await fetch(`https://companies.stream/${stream}/timepoint`);
if (!timepointRes.ok)
  throw new Error(
    `Failed to fetch timepoint for ${stream} stream: ${timepointRes.status} ${timepointRes.statusText}`,
  );
const timepointJson = (await timepointRes.json()) as { min: number; max: number };

const startTestFrom = Math.max(timepointJson.max - limit, timepointJson.min);
console.log("Starting test from", startTestFrom, "on", stream, "stream");
console.log("Timepoint range", timepointJson.min, "to", timepointJson.max);
const url = `https://companies.stream/${stream}?timepoint=${startTestFrom}`;

console.time("Stream");
const start = Date.now();
const ac = new AbortController();
const { signal } = ac;
const res = await fetch(url, { signal });
console.log("Response code", res.status, res.statusText);

const events = Readable.fromWeb(res.body!, { signal }).pipe(split2(), { end: true });

let counter = 0;
let timepointTracker = startTestFrom;

let ttfb;
for await (const rawEvent of events) {
  if (!ttfb) {
    ttfb = Date.now() - start;
    console.log("Time to first byte", ttfb, "ms");
  }
  try {
    const parsedEvent = JSON.parse(rawEvent);
    if (limit < 1000) process.stdout.write(".");
    if (parsedEvent.event.timepoint !== timepointTracker++) {
      process.stdout.write("-");
      timepointTracker = parsedEvent.event.timepoint + 1;
    }
  } catch (e) {
    process.stdout.write("x");
    console.error("Error parsing event", rawEvent, e);
  }
  counter++;
  // To test cancelling a request
  if (counter % 100 === 0) {
    ac.abort();
    break;
  }
  if (counter % 100_000 === 0) console.timeLog("Stream", counter);
}

const duration = Date.now() - start;
console.log();
console.timeEnd("Stream");
console.log("Done", counter, "events");
console.log("Finished at Timepoint", timepointTracker);
console.log("Events per second:", (counter / duration) * 1000, "events/s");
