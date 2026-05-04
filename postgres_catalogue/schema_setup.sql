
create schema if not exists ducklake;

grant all on schema ducklake to "read-write";

grant select on ALL TABLES IN schema ducklake to "read-only";


create schema if not exists cc_metadata;

grant all on schema cc_metadata to "read-write";

grant select on ALL TABLES IN schema cc_metadata to "read-only";