# API lab Bab 6: hanya pustaka standar Go.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN go vet ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api .

FROM alpine:3.20
COPY --from=build /out/api /app/api
# Direktori volume dibuat dengan pemilik uid 50000 agar volume bernama baru mewarisi pemilik ini.
RUN mkdir -p /data /opt/lab/dbt_target && chown 50000:0 /data /opt/lab/dbt_target
# uid 50000 = user airflow, agar volume bronze/dbt bisa ditulis dan dibaca kedua container
USER 50000:0
EXPOSE 8080
ENTRYPOINT ["/app/api"]
