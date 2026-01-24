import {DuckDBInstance} from "@duckdb/node-api";

const db = await DuckDBInstance.create('./index.db');
export const connection = await db.connect();
