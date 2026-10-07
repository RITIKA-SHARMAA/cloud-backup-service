# One image holds all four binaries; each Deployment chooses which one to run.

# --- build stage: compile static binaries -----------------------------------------------
FROM golang:1.24-alpine AS build
WORKDIR /src
# Download modules first, in their own layer, so code changes do not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# --- runtime stage: small image with pg_dump/pg_restore for PostgreSQL backups -------------
FROM alpine:3.21
# pg_dump must be the same major version as the server or newer; 17 can dump servers up to 17.
RUN apk add --no-cache ca-certificates postgresql17-client \
 && adduser -D -u 10001 cbs
COPY --from=build /out/ /usr/local/bin/
# Run as a non-root user.
USER 10001
WORKDIR /home/cbs
EXPOSE 8080
ENTRYPOINT []
CMD ["api"]
