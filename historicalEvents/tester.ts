import split2 from "split2";
import {Readable} from "node:stream";

const earliestCharges = '3508711'
const earliestCompanies = '105420168'
const latestCompanies = 107315293
console.time('Stream')
const start = Date.now()
// const url = `https://companies.stream/charges?timepoint=${earliestCharges}`
const url = `https://companies.stream/companies?timepoint=${latestCompanies - 120_000}`

const res = await fetch(url)
console.log('Response code', res.status, res.statusText)
const events = Readable.fromWeb(res.body!).pipe(split2())

let counter = 0;
let timepointTracker :number = 0
for await (const rawEvent of events) {
    try{
        const parsedEvent = JSON.parse(rawEvent)
        if(parsedEvent.event.timepoint === timepointTracker++) {
            // process.stdout.write('.')
        }else{
            process.stdout.write('-')
            timepointTracker = parsedEvent.event.timepoint + 1
        }

    }catch (e) {
        process.stdout.write('x')
        // console.log()
        // console.log('Parse failure:','"'+rawEvent+'"')
    }
    counter++
    if(counter % 100_000 === 0) console.timeLog('Stream',counter)
}

const duration = Date.now() - start
console.log()
console.timeEnd('Stream')
console.log('Done', counter, 'events')
console.log('Finished at Timepoint', timepointTracker)

console.log('Events per second:', counter / duration * 1000, 'events/s')