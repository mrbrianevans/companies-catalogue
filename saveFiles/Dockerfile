# Build stage
FROM golang:1.25-alpine AS build
WORKDIR /usr/src/companies-catalogue
# Copy only the saveFiles directory context
COPY . .
# Download deps and build static binary without BuildKit cache mounts
RUN go mod download && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /savefiles

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache pandoc poppler-utils libreoffice

WORKDIR /
COPY --from=build /savefiles /savefiles
ENTRYPOINT ["/savefiles"]
