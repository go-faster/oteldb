FROM golang:latest as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -o /app/oteldb ./cmd/oteldb

FROM clickhouse/clickhouse-server

WORKDIR /app
COPY --from=builder /app/oteldb /oteldb

VOLUME /clickhouse
ENV EMBEDDED_CLICKHOUSE_HOST=0.0.0.0

ENTRYPOINT ["/oteldb"]
