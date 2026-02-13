import { DuckDBInstance } from "@duckdb/node-api";

const db = await DuckDBInstance.create("./index.db");
export const connection = await db.connect();
await connection.run(`
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET s3_sink (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? "").host}'
);
`);
