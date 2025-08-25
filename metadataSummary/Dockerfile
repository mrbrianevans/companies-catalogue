FROM oven/bun:1 AS builder
WORKDIR /usr/src/companies-catalogue

# install dependencies
# this will cache them and speed up future builds
# build a compiled executable
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun build summarise.ts --outfile summarise --compile

# copy executable and run
FROM gcr.io/distroless/cc-debian12 AS run
COPY --from=builder /usr/src/companies-catalogue/summarise /summarise

ENTRYPOINT [ "/summarise", "/output/sftp_catalogue.db", "/output/sftp_file_metadata_summary.json" ]
