// to allow importing sql files as strings with bun: eg
// import sqlString from "./query.sql" with { type: "text" };
declare module "*.sql" {
  export default string;
}
