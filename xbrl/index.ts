


import {$} from 'bun'
import { s3Client } from "../eventCapture/utils.ts";

console.time('Write CSV from XBRL ZIP URL')

const url = `https://download.companieshouse.gov.uk/Accounts_Monthly_Data-July2026.zip`

const outStream = $`ch-xbrl ${url}`

const outFile = s3Client.file('./ch-xbrl/2026-07-01--2026-07-31.csv.zst')

const rawOutput = await outStream.arrayBuffer()
const compressedOutput = await Bun.zstdCompress(rawOutput)
await outFile.write(compressedOutput)

console.timeEnd('Write CSV from XBRL ZIP URL')
