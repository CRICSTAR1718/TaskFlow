# One image, five binaries. docker-compose picks which one to run with
# `command:` (./api, ./worker, ./scheduler, ./recovery, ./migrate) because the
# entrypoint is a tiny shim that execs "$1". That way the layers are shared and
# `docker compose build` builds once instead of five times.
#
# It still works on its own:
#   docker build -f deploy/Dockerfile .                   # runs ./api by default
#   docker run taskflow:local ./worker                    # any other service
FROM golang:1.24-alpine AS build
WORKDIR /src

# go.mod first: dependency layers stay cached when only source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN for s in api worker scheduler recovery migrate; do \
        target=./cmd/$s; [ -d "$target" ] || target=./$s; \
        CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$s "$target" || exit 1; \
    done

# Static binaries, network-only dependencies (Postgres and Redis), nonroot user.
FROM alpine:3.20
RUN adduser -D -u 65532 app
COPY --from=build /out/ /out/
# alpine's busybox sh is the shim: `./api`, `./worker`, ... resolve against /out.
WORKDIR /out
USER app
ENTRYPOINT ["/bin/sh", "-c", "exec \"$@\"", "--"]
CMD ["./api"]
