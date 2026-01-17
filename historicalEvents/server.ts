import { getHistoricalStream } from "./handler.js";
import { getMinMaxRange } from "./fileIndex.js";

const streams = ['companies', 'filings', 'officers', 'persons-with-significant-control', 'charges', 'insolvency-cases', 'disqualified-officers', 'company-exemptions', 'persons-with-significant-control-statements']
const makeError = (code: number, message: string) => Response.json({error: message}, {status: code})

const server = Bun.serve({
    routes: {
        '/:path': async (request, _server) => {
            try {
                const path = request.params.path
                const timepointInputString = new URL(request.url).searchParams.get('timepoint')

                // Request validation
                if (!streams.includes(path))
                    return makeError(400, 'Invalid stream. Options: ' + streams.join(', '))
                if (!timepointInputString)
                    return makeError(400, 'Missing timepoint parameter')
                const timepoint = Number(timepointInputString)
                if (isNaN(timepoint))
                    return makeError(400, 'Invalid timepoint parameter')

                const validRange = await getMinMaxRange(path)
                if (!validRange)
                    return makeError(501, 'No data available for ' + path)

                if (timepoint > validRange.max)
                    return makeError(416, 'Timepoint out range (too big)')
                if (timepoint < validRange.min)
                    return makeError(416, 'Timepoint out range (too small)')

                const outputStream = await getHistoricalStream(path, timepoint)
                return new Response(outputStream)
            }catch(error){
                console.error('Internal error',error)
                return makeError(500, 'Internal server error')
            }
        }
    }
})
console.log('Server listening on', server.url.href)