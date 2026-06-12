import { setupLakehouseConnection } from "../lakehouse/connection.ts";
import { executeSql } from "../lakehouse/utils.ts";

async function loadCharityEntities() {
  const { connection } = await setupLakehouseConnection();
  await connection.run(`
use lakehouse;
create schema if not exists charity;
use lakehouse.charity;
INSTALL httpfs;
LOAD httpfs;
INSTALL zipfs FROM community;
LOAD zipfs;
`);
  const entities = [
    "charity",
    "charity_annual_return_history",
    "charity_annual_return_parta",
    "charity_annual_return_partb",
    "charity_area_of_operation",
    "charity_classification",
    "charity_event_history",
    "charity_governing_document",
    "charity_other_names",
    "charity_other_regulators",
    "charity_policy",
    "charity_published_report",
    "charity_trustee",
  ];
  for (const entity of entities) {
    await executeSql(
      connection,
      `
        CREATE OR REPLACE TABLE ${entity} AS (
            SELECT * FROM read_csv(
                'zip://https://ccewuksprdoneregsadata1.blob.core.windows.net/data/txt/publicextract.${entity}.zip/publicextract.${entity}.txt'
            )
        );
`,
    );
  }
}

await loadCharityEntities();
