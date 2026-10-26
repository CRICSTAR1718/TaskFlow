# Builds any service: docker build -f deploy/Dockerfile --build-arg SERVICE=api .
FROM golang:1.22-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
