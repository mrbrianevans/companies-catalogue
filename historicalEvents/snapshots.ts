// Export snapshots from the datalake for convenient download

import {streams} from "./utils.js";
import {setupLakehouseConnection} from "./connection.js";
import {platform} from "node:os";

const getSchema = (streamPath: string) => streamPath.replaceAll(/[^a-z0-9_]/gi, '_')

const snapshotBucket = process.env.SNAPSHOT_BUCKET

async function main(streamPath: string) {
    if (!streams.includes(streamPath)) {
        console.log('stream', streamPath, 'not in streams list, skipping')
        return
    }
    console.log('Exporting', streamPath, 'snapshots')
    console.time('setup local catalogue')
    const {connection} = await setupLakehouseConnection()
    console.timeEnd('setup local catalogue')

    await connection.run(`USE lakehouse.${getSchema(streamPath)};`)

    const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`)
    const tables = tablesRes.getRowObjects().map(r => r.name as string)
    console.log('tables in lakehouse', tables)

    await connection.run(`ATTACH 'temp.db' as local;`)
    await connection.run(`CREATE SCHEMA IF NOT EXISTS local.${getSchema(streamPath)};`)

    console.time('create local snapshot from lakehouse')
    await connection.run(`
    CREATE OR REPLACE TABLE local.${getSchema(streamPath)}.snapshot AS 
    SELECT * FROM lakehouse.${getSchema(streamPath)}.snapshot;
    `)
    console.timeEnd('create local snapshot from lakehouse')

    await connection.run(`SET preserve_insertion_order = true;`)

    // export local snapshot to various formats on S3
    const fileTypes = [{format: 'json', compression: 'none', extension: '.json'}, {
        format: 'parquet', compression: 'snappy', extension: '.parquet'
    }, {format: 'json', compression: 'gzip', extension: '.json.gz'}, {
        format: 'json', compression: 'zstd', extension: '.json.zst'
    }]


    //one file
    for (const file of fileTypes) {
        console.time('export ' + file.extension)
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}${file.extension}'
        (FORMAT ${file.format}, COMPRESSION ${file.compression});
        `)
        console.timeEnd('export ' + file.extension)
    }

    // vortex experiment
    if (platform() !== 'win32') {
        console.time('export vortex')
        await connection.run(`
        INSTALL vortex;
        LOAD vortex;
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.vortex'
        (FORMAT vortex);
        `)
        console.timeEnd('export vortex')
    }

// split files
    const fileSizeBytes = 512 * 1024 * 1024
    for (const f of fileTypes) {
        console.time('export split ' + f.extension)
        await connection.run(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
            TO 's3://${snapshotBucket}/split/${streamPath}'
            (FORMAT ${f.format}, OVERWRITE_OR_IGNORE true, COMPRESSION ${f.compression}, FILE_SIZE_BYTES ${fileSizeBytes}, FILENAME_PATTERN '${streamPath}_part_{i}');
            `)
        console.timeEnd('export split ' + f.extension)
    }


    //sample of 1000 items
    for (const f of fileTypes) {
        console.time('export sample ' + f.extension)
        await connection.run(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot USING SAMPLE 1000) 
            TO 's3://${snapshotBucket}/sample/${streamPath}${f.extension}'
            (FORMAT ${f.format}, COMPRESSION ${f.compression});
            `)
        console.timeEnd('export sample ' + f.extension)
    }


    /*
    TODO:
     - change output path to an s3 bucket
     - add a json manifest to the root dir of bucket with list of files. RETURN_FILES
     - could use variables and prepared statements to reduce repetition?
     */
    /*
    Other formats to consider:
     - CSV via MongoDB (for nested headers)
     - Avro
     - Zip archive compression
     */
    connection.closeSync()
}

await main(process.argv[2])