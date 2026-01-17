import {getHistoricalStream} from "./handler.js";
import {getMinMaxRange} from "./fileSequence.js";
import * as stream from "node:stream";

const streams = ['companies', 'filings', 'officers', 'persons-with-significant-control', 'charges', 'insolvency-cases', 'disqualified-officers', 'company-exemptions', 'persons-with-significant-control-statements']
const invalidStreamMessage = 'Invalid stream. Valid '+streams.join(', ')

const server = Bun.serve({
    routes: {
        '/:path': async (request, _server) => {
            const path =  request.params.path
            const timepointInputString = new URL(request.url).searchParams.get('timepoint')

            // Request validation
            if(!streams.includes(path))
                return Response.json({error: invalidStreamMessage}, {status:400})
            if(!timepointInputString)
                return Response.json({error: 'Missing timepoint parameter'}, {status: 400})
            const timepoint = Number(timepointInputString)
            if(isNaN(timepoint))
                return Response.json({error: 'Invalid timepoint parameter'}, {status: 400})

            const validRange = await getMinMaxRange(path)
            if(!validRange)
                return Response.json({error: 'No data available for '+path}, {status: 501})
            if(timepoint > validRange.max || timepoint < validRange.min)
                return Response.json({error: 'Sorry, timepoint out of range'}, {status: 416})

            const outputStream = await getHistoricalStream(path, timepoint)
            return new Response(outputStream)
        }
    }
})
console.log('Server listening on', server.url.href)