import {setupLakehouseConnection} from "../lakehouse/connection.ts";
import {executeSql} from "../lakehouse/utils.ts";



async function loadCharityEntities(){

    const {connection} = await setupLakehouseConnection()
await connection.run(`
use lakehouse;
create schema if not exists charity;
use lakehouse.charity;
INSTALL httpfs;
LOAD httpfs;
INSTALL zipfs FROM community;
LOAD zipfs;
`)
    const entities = [
        'charity_other_regulators'
    ]
    for(const entity of entities){
    await executeSql(connection, `
        CREATE TEMPORARY TABLE ${entity} AS (
            SELECT * FROM read_json_objects(
                'zip://https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/publicextract.${entity}.zip/publicextract.${entity}.json',
                format = 'array'
            )
        );
`)
}
}


await loadCharityEntities()