# Streaming Log Analytics Pipeline

A high-throughput streaming log analytics pipeline. Fake HTTP access log events flow from Go producers through into Kafka are converted into Parquet files sent to on MinIO with consumers. Queries are in real time via DuckDB

## Architecture

```
┌─────────────────┐  http.logs  ┌──────────────────┐
│    Producer     │────────────▶│      Kafka       │
└─────────────────┘             └────────┬─────────┘
                                         │
                                         ▼
                                ┌─────────────────┐
                                │    Consumers    │
                                └────────┬────────┘
                                         │ Parquet (Snappy compression)
                                         ▼
                                 ┌───────────────┐
                                 │     MinIO     │
                                 └───────▲───────┘
                                         │ S3 / httpfs
                              docker compose run consumer query
                              └── DuckDB (embedded in binary)
```

Events are partitioned on MinIO using Hive-style paths:

```
logs/data/year=YYYY/month=MM/day=DD/hour=HH/part-NNNN.parquet
```

DuckDB is runs via docker `docker compose run --rm consumer query` starts the image with the `query` subcommand; DuckDB then connects to MinIO over S3/httpfs and queries the Parquet files directly.

## Prerequisites

- Docker Compose
- Go 1.25+ 
- CGO required for DuckDB



## Getting Started

```bash
./setup.sh
```

`setup.sh` builds the image, starts all services in dependency order, waits for healthchecks, creates the Kafka topic (4 partitions) and MinIO bucket, then starts one producer and four consumers.

The topic has 4 partitions and the consumer group runs 4 members, so each consumer owns exactly one partition.

## Services


| Service    | URL                                            | Credentials             |
| ---------- | ---------------------------------------------- | ----------------------- |
| Grafana    | [http://localhost:3000](http://localhost:3000) | admin / admin           |
| MinIO      | [http://localhost:9001](http://localhost:9001) | minioadmin / minioadmin |
| Prometheus | [http://localhost:9090](http://localhost:9090) |                         |




## Queries

Queries run via a short-lived container using DuckDB:

```bash
make query ARGS="<subcommand> [flags]"
```


| Subcommand          | Flags                        | Default window | Description                        |
| ------------------- | ---------------------------- | -------------- | ---------------------------------- |
| `status-codes`      |                              | —              | Request count per HTTP status code |
| `top-paths`         | `--limit N` `--window Xh/Xm` | `1h`           | Top URIs by request count          |
| `top-clients`       | `--limit N` `--window Xh/Xm` | `1h`           | Top client IDs by request count    |
| `error-rate`        | `--granularity day`          | hour           | minute `--window Xh/Xm`            |
| `request-volume`    | `--granularity day`          | hour           | minute `--window Xh/Xm`            |
| `bytes-transferred` | `--granularity day`          | hour           | minute `--window Xh/Xm`            |




### Examples

```bash
# Which status codes are being returned?
make query ARGS="status-codes"
# ┌─────────────┬──────────┐
# │ STATUS CODE │ REQUESTS │
# ├─────────────┼──────────┤
# │ 200         │ 6697026  │
# │ 304         │ 2507819  │
# │ 404         │ 1675223  │
# │ 500         │  838694  │
# └─────────────┴──────────┘

# Top 5 paths in the last 30 minutes
make query ARGS="top-paths --limit 5 --window 30m"
# ┌──────────────────┬──────────┐
# │       URI        │ REQUESTS │
# ├──────────────────┼──────────┤
# │ /account/login   │  668835  │
# │ /robots.txt      │  668228  │
# │ /api/v1/products │  668053  │
# │ /cart            │  667655  │
# │ /checkout        │  667236  │
# └──────────────────┴──────────┘

# Top 5 clients in the last 15 minutes
make query ARGS="top-clients --limit 5 --window 15m"

# Hourly error rate over the last 6 hours
make query ARGS="error-rate --granularity hour --window 6h"

# Request volume by minute over the last hour
make query ARGS="request-volume --granularity minute --window 1h"

# MB transferred per hour over the last 24 hours
make query ARGS="bytes-transferred --granularity hour --window 24h"
```



## Dashboard Layout


| Row         | Panels                                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------------------- |
| Producer    | Publish rate, Kafka publish latency p99, publish errors                                                       |
| Consumer    | Consume rate, batch flush size, Parquet write latency p99, MinIO upload latency p99, upload errors, Kafka lag |
| Application | CPU, memory RSS, goroutines per container                                                                     |




## Configuration

Key environment variables (set in `docker-compose.yml` or overridden at runtime):


| Variable                  | Default          | Description                                   |
| ------------------------- | ---------------- | --------------------------------------------- |
| `KAFKA_BROKERS`           | `localhost:9092` | Kafka bootstrap address                       |
| `PRODUCER_RATE_LIMIT_RPS` | `15000`          | Max events/sec per producer (0 = unlimited)   |
| `MAX_EVENTS`              | `1000000`        | Producer stops after N events (0 = unlimited) |




## Teardown

```bash
make down
```

Stops all containers and removes volumes
