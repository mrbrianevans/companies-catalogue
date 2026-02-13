import { handleStreamRequest } from "./handler.js";
import { getMinMaxRange } from "./readFileIndex.js";
import { makeError, streams } from "../lakehouse/utils";
import { updateIndex } from "./writeFileIndex.js";
import { connection } from "./duckdbConnection";

// set a "cron" interval in bun to refresh the index every day
setInterval(() => updateIndex(connection).catch(console.error), 86400_000);

const server = Bun.serve({
  port: 3000,
  hostname: "10.106.0.5", // only accessible via private IP
  routes: {
    // This allows consumers to check what a valid range of timepoint to request is.
    "/:path/timepoint": async (request) => {
      const path = request.params.path;
      if (!streams.includes(path))
        return makeError(400, "Invalid stream. Options: " + streams.join(", "));
      console.log(new Date(), "Handling timepoint request for", path);
      const validRange = await getMinMaxRange(path);
      if (!validRange) return makeError(501, "No data available for " + path);

      return Response.json(validRange);
    },

    // This is the streaming endpoint. Path must be /filings, /companies etc.
    "/:path": async (request) => {
      //TODO: handle HEAD requests. don't think we can work out content-length since the source is gzipped unfortunately.
      const path = request.params.path;
      const timepointInputString = new URL(request.url).searchParams.get("timepoint");
      return handleStreamRequest(path, timepointInputString, request.signal);
    },
  },
});
console.log(new Date(), "Server listening on", server.url.href);

// Track process exits
process.on("SIGINT", () => {
  console.log(new Date(), "Exiting due to SIGINT");
  process.exit(0);
});
process.on("SIGTERM", () => {
  console.log(new Date(), "Exiting due to SIGTERM");
  process.exit(0);
});
process.on("SIGHUP", () => {
  console.log(new Date(), "Exiting due to SIGHUP");
  process.exit(0);
});
process.on("exit", () => console.log(new Date(), "Exiting process"));
process.on("uncaughtException", (err) => {
  console.error("uncaughtException", err);
});
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});
