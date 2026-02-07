// Export snapshots from the datalake for convenient download

import {streams} from "./utils.js";
import {setupLakehouseConnection} from "./connection.js";
import {platform} from "node:os";
import {DuckDBListValue} from "@duckdb/node-api";

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
    const outputFiles = []
    // export local snapshot to various formats on S3
    const fileTypes = [{format: 'json', compression: 'none', extension: '.json'}, {
        format: 'parquet', compression: 'snappy', extension: '.parquet'
    }, {format: 'json', compression: 'gzip', extension: '.json.gz'}, {
        format: 'json', compression: 'zstd', extension: '.json.zst'
    }]


    //one file
    for (const file of fileTypes) {
        console.time('export ' + file.extension)
        const filesRes = await connection.runAndReadAll(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}${file.extension}'
        (FORMAT ${file.format}, COMPRESSION ${file.compression}, RETURN_FILES true);
        `)
        console.timeEnd('export ' + file.extension)
        outputFiles.push(...filesRes.getRowObjects().map(f => ({files: (f.Files as DuckDBListValue).items as string[], count: Number(f.Count)})))
    }

    // vortex experiment
    if (platform() !== 'win32') {
        console.time('export vortex')
        const filesRes = await connection.runAndReadAll(`
        INSTALL vortex;
        LOAD vortex;
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.vortex'
        (FORMAT vortex, RETURN_FILES true);
        `)
        console.timeEnd('export vortex')
        outputFiles.push(...filesRes.getRowObjects().map(f => ({files: (f.Files as DuckDBListValue).items as string[], count: Number(f.Count)})))
    }

// split files
    const fileSizeBytes = 512 * 1024 * 1024
    for (const f of fileTypes) {
        //TODO: clean out existing files from path in bucket in case snapshot size decreases and leaves an old partition. (low risk)
        console.time('export split ' + f.extension)
        const filesRes = await connection.runAndReadAll(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
            TO 's3://${snapshotBucket}/split/${streamPath}'
            (FORMAT ${f.format}, OVERWRITE_OR_IGNORE true, COMPRESSION ${f.compression}, FILE_SIZE_BYTES ${fileSizeBytes}, FILENAME_PATTERN '${streamPath}_part_{i}', RETURN_FILES true);
            `)
        console.timeEnd('export split ' + f.extension)
        outputFiles.push(...filesRes.getRowObjects().map(f => ({files: (f.Files as DuckDBListValue).items as string[], count: Number(f.Count)})))
    }


    //sample of 1000 items
    for (const f of fileTypes) {
        console.time('export sample ' + f.extension)
        const filesRes = await connection.runAndReadAll(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot USING SAMPLE 1000) 
            TO 's3://${snapshotBucket}/sample/${streamPath}${f.extension}'
            (FORMAT ${f.format}, COMPRESSION ${f.compression}, RETURN_FILES true);
            `)
        console.timeEnd('export sample ' + f.extension)
        outputFiles.push(...filesRes.getRowObjects().map(f => ({files: (f.Files as DuckDBListValue).items as string[], count: Number(f.Count)})))
    }

    console.log('Exported files', JSON.stringify(outputFiles))
    const manifest = {
        streamPath,
        publishedAt: new Date().toISOString(),
        downloads: outputFiles.flatMap(f => f.files)
    }
    await Bun.s3.write(`${streamPath}-manifest.json`, JSON.stringify(manifest), ({bucket: snapshotBucket, type: 'application/json'}))
    console.log('Manifest uploaded to S3')
    /*
    Other formats to consider in future:
     - CSV via MongoDB (for nested headers)
     - Avro, ORC, Feather, Lance
     - Zip archive compression
     */
    connection.closeSync()
}

await main(process.argv[2])