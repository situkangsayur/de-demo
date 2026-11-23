# API lab Bab 7 (Go 1.24): pgx untuk PostgreSQL, go-redis untuk Redis; ClickHouse dan OpenSearch lewat HTTP.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go vet ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api .

FROM alpine:3.20
RUN adduser -D -u 10001 lab
COPY --from=build /out/api /app/api
USER lab
EXPOSE 8080
ENTRYPOINT ["/app/api"]
