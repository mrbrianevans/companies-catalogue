
create schema ducklake;

grant all on schema ducklake to "read-write";

grant select on ALL TABLES IN schema ducklake to "read-only";