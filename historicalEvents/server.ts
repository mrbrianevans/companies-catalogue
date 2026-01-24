import { handleStreamRequest} from "./handler.js";
import { getMinMaxRange } from "./readFileIndex.js";
import {makeError, streams} from "./utils.js";
import {updateIndex} from "./writeFileIndex.js";
import {connection} from "./duckdbConnection.js";

// set a "cron" interval in bun to refresh the index every day
setInterval(()=>
    updateIndex(connection)
,86400_000)

const server = Bun.serve({
    routes: {
        // This allows consumers to check what a valid range of timepoint to request is.
        '/:path/timepoint': async (request)=>{
            const path = request.params.path
            if (!streams.includes(path))
                return makeError(400, 'Invalid stream. Options: ' + streams.join(', '))
            const validRange = await getMinMaxRange(path)
            if (!validRange)
                return makeError(501, 'No data available for ' + path)

            return Response.json(validRange)
        },

        // This is the streaming endpoint. Path must be /filings, /companies etc.
        '/:path': async (request) => {
            //TODO: handle HEAD requests. don't think we can work out content-length since the source is gzipped unfortunately.
            const path = request.params.path
            const timepointInputString = new URL(request.url).searchParams.get('timepoint')
            return handleStreamRequest(path, timepointInputString, request.signal)
        }
    }
})
console.log('Server listening on', server.url.href)