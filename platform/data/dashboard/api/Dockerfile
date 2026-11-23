# API dashboard: hanya pustaka standar Go (ClickHouse lewat HTTP interface, lake-engine dan Airflow lewat REST).
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN go vet ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dash-api .

FROM alpine:3.20
RUN adduser -D -u 10001 dash
COPY --from=build /out/dash-api /app/dash-api
USER dash
EXPOSE 8080
ENTRYPOINT ["/app/dash-api"]
