import {getHistoricalStream} from "./handler.js";


const server = Bun.serve({
    routes: {
        '/:path': async (request, _server) => {
            const path =  request.params.path
            const timepoint = new URL(request.url).searchParams.get('timepoint')
            if(!timepoint) return new Response('Missing timepoint parameter', {status: 400})
            const outputStream = await getHistoricalStream(path, Number(timepoint))
            return new Response(outputStream)
        }
    }
})
console.log('listening', server.url.href)