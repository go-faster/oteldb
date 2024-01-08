FROM golang:latest as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /otelbench ./cmd/otelbench

FROM alpine:latest
RUN apk --no-cache add ca-certificates

# Some dependencies that otelbench promrw launches with os.Exec:
COPY --from=victoriametrics/vmagent /vmagent-prod /bin/vmagent
COPY --from=prom/prometheus /bin/prometheus /bin/promtool
COPY --from=quay.io/prometheus/node-exporter /bin/node_exporter /bin/node_exporter

COPY --from=builder /otelbench /bin/otelbench

ENTRYPOINT ["/bin/otelbench"]
