function parseLog(logContent) {
    const regex = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\sFound path\s(.+)$/gm;
    const result = [];

    let match;
    while ((match = regex.exec(logContent)) !== null) {
        const [, timestamp, path] = match;
        if(path.startsWith('/free/bulkimage')) continue;
        const pathParts = path.match(/\/free\/(prod\w+)\/(\d{4})\/(\d{2})\/(\d{2})\/.+$/) || [];

        result.push({
            timestamp,
            path,
            prodNumber: pathParts[1] || null,
            year: pathParts[2] || null,
            month: pathParts[3] || null,
            day: pathParts[4] || null
        });
    }

    return result;
}

import {readFile} from 'fs/promises';
const log = await readFile('./companies-catalogue-logs-16-08.txt', 'utf8')

const lines = (parseLog(log));
console.log(lines.slice(0, 2));

const stats = {
    prodNumber: {},
    year: {}
};

lines.forEach(item => {
    if (item.prodNumber) {
        stats.prodNumber[item.prodNumber] = (stats.prodNumber[item.prodNumber] || 0) + 1;
    }
    if (item.year) {
        stats.year[item.year] = (stats.year[item.year] || 0) + 1;
    }
});

console.log('Rows by prodNumber:', stats.prodNumber);
console.log('Rows by year:', stats.year);
console.log('Total rows:', lines.length);
console.log('Unmatched:', lines.filter(line=>!line.prodNumber).length);
console.log('Bulk images', lines.filter(line=>line.path.startsWith('/free/bulkimage')).length);
let counter = 0
for(const line of lines){
    if(!line.prodNumber){
        console.log(line.path);
        // if(counter++ > 10)break
    }
}