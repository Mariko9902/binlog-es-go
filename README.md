# binlog-es-go

## Overview

`binlog-es-go` synchronizes MySQL data to Elasticsearch using binlog events. It supports multi-table joins, complex SQL queries, and field transformations. It was developed as an alternative to Canal for queries involving subqueries and reverse lookups.

## Features

- Real-time incremental synchronization from MySQL binlog.
- Full or partial recomputation using bootstrap mode.
- SQL mappings, field transformations, and reverse lookups from related tables.
- Prometheus metrics and configurable retry, batching, and circuit-breaking behavior.
- Table-name rewriting and sharding for partitioned tables.
- Elasticsearch 7, 8, and 9 support through the HTTP REST API.
- HTTPS, custom CA certificates, and mutual TLS. Disabling TLS verification is intended only for development.

## Build and run

Run the following commands from the project root:

```bash
go build -o bin/binlog-es-go ./cmd/binlog-es-go
./bin/binlog-es-go --config=configs/config.yaml --mode=realtime --task=<task_name>
```

The real-time process exposes `/healthz` and Prometheus metrics at `/metrics` on port 8222 by default. The main batching and timeout settings are `syncTasks[].bulk.size`, `syncTasks[].bulk.flushIntervalMs`, `syncTasks[].bulk.concurrent`, `realtime.maxPending`, `realtime.queryTimeoutMs`, and `realtime.esBulkTimeoutMs`.

When neither a file position nor GTID is configured or saved, the program reads the current binlog position. Earlier MySQL versions use `SHOW MASTER STATUS`; MySQL 8.4 uses `SHOW BINARY LOG STATUS`. The account needs `REPLICATION CLIENT` and typically `REPLICATION SLAVE` privileges.

## Bootstrap

```bash
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name>

./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name> \
  --bootstrap.partition.auto_id.min=100000 --bootstrap.partition.auto_id.max=200000

./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name> \
  --bootstrap.sql-where="s.AutoID = 305681"

./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name> \
  --bootstrap.sql-where="s.AutoID IN (305681,305682,305700)"
```

Without bounds, bootstrap scans primary keys with cursor pagination (`ORDER BY` and `LIMIT`), including non-incrementing or snowflake IDs. Configured shards are scanned concurrently. With bounds, the interval is `[min, max)`. `bootstrap.partitionSize` is the page size for a full scan and the partition size for a bounded scan.

Concurrency and batching are controlled by `bootstrap.partitionSize`, `bootstrap.workers`, `bootstrap.bulkSize`, `bootstrap.bulkFlushMs`, `bootstrap.runBatchSize`, and `syncTasks[].bulk.size`.

## Configuration

See `configs/config.example.yaml` for the configuration template. The main fields are `dataSource.dsn`, `es.addresses`, `syncTasks[].mapping`, `syncTasks[].mappingTable`, and `syncTasks[].relatedQuery`.

- `logging.level` and `logging.file` configure logging; an empty file path logs to the console.
- `debug.sql` logs SQL and `debug.sqlParams` logs parameter samples.
- `server.port` configures the HTTP server.
- `syncTasks[].destination` names the task.
- `mapping._index` and `mapping._id` select the index and document ID field. `upsert` controls upserts.
- `mapping.sql` is the main query. `IN (?)` is recommended; named placeholders such as `IN(:keys)` and `IN(:auto_ids)` are also supported.
- `mappingTable` maps tables to columns used to extract primary document IDs. `relatedQuery` maps child-table changes back to primary IDs.
- `transforms` applies field transformations such as `splitFields` before indexing.
- `bulk` configures thresholds, flush intervals, and concurrency. `retry` configures attempts and backoff for SQL and Elasticsearch writes.

### Deletion

`mapping.deleteOnDelete` deletes the corresponding Elasticsearch document when the main table emits a `DELETE_ROWS` event. Child-table deletions trigger recomputation instead.

`mapping.deleteOnMissing` deletes IDs for which the main SQL query returns no rows. This can be useful for physical deletions or rows excluded by the SQL filter. Delayed or out-of-order writes can temporarily make a row unavailable and cause an unintended deletion. Enable it only when a missing query result means the document should be deleted; otherwise use `deleteOnDelete` alone. The two options are independent.

### Sharded tables

Use `syncTasks[].tableRewrite` to map physical binlog table names such as `enterprise_07` to a logical name such as `enterprise` before matching `mappingTable` and `relatedQuery`. Configure `mapping.sharding` and use SQL template variables such as `{{enterprise_table}}` and `{{enterprise_event_table}}` to query physical tables. Set `mapping.mainTable` explicitly when the main table cannot be parsed from SQL, for example when `FROM` uses a template variable.

Sharding keys are represented as decimal strings, encoded in UTF-8. Supported `sharding.strategy` values are:

- `crc32_ieee_uint32` (default): `crc32_ieee_uint32(utf8(key_string)) % shards`.
- `mod`: `abs(key) % shards`.
- `crc32_ieee_signed_abs`: `abs(int32(crc32_ieee_uint32(bytes))) % shards`.
- `crc32`: an alias for `crc32_ieee_signed_abs`, retained for existing configurations.

For `shards=64`, the expected shard numbers are `0 → 33`, `1 → 55`, `42 → 08`, `258652761531355136 → 00`, and `258652848596717568 → 63`.

For a main/child table model, map the main table's primary key, for example `mappingTable.enterprise: ["id"]`. Do not map a child table's own `id` as the main document ID: use `relatedQuery` and its foreign key, such as `enterprise_id`, to find the main IDs. A mistaken mapping can cause incorrect deletions when `deleteOnMissing` is enabled.

### Command-line options

| Option | Description |
| --- | --- |
| `--config` | Configuration path; default `configs/config.yaml` |
| `--mode` | `realtime` or `bootstrap`; default `realtime` |
| `--task` | Task name |
| `--bootstrap.partition.auto_id.min` / `.max` | Bootstrap ID bounds |
| `--bootstrap.partition.size` | Full-scan page size or range partition size; default 5000 |
| `--bootstrap.workers` | Bootstrap workers; default 4 |
| `--bootstrap.sql-where` | Additional bootstrap WHERE condition |
| `--bulk.size` | Override batch size; default 1000 |
| `--bulk.flush-ms` | Override flush interval in milliseconds; default 800 |
| `--log.level` | Override log level: `debug`, `info`, `warn`, or `error` |
| `--log.file` | Override log path |

## Operations and troubleshooting

- Use `relatedQuery.pageSize` and a unique, increasing `orderKey` for cursor pagination. Index the filter and order columns together. `realtime.relatedQueryMaxPages` limits the number of pages, and `realtime.maxPending` protects memory by forcing a flush.
- Index SQL join and filter columns. For example, a category lookup can use `(Category_id, AutoID)`.
- The main SQL can use `GROUP BY s.AutoID` for aggregation when `ONLY_FULL_GROUP_BY` is enabled; verify that the query complies with the server's SQL mode.
- `es.refresh` accepts `""`, `"false"`, `"wait_for"`, or `"true"`. An unset value uses the index default.
- Larger batches and flush intervals can improve throughput but increase latency. Monitor binlog lag and Elasticsearch latency when tuning.
- SQL and Elasticsearch writes use configured retries. Elasticsearch opens a circuit after repeated failures, with backoff capped by `realtime.esCircuitMaxBackoffMs`. Version conflicts trigger one recomputation and retry.
- Bootstrap failures and failed real-time conflict recomputations are written to `logs/dead-letters/<task>.log`. Run `./scripts/analyze-failures.sh <task>` to analyze failures or use `--mode=replay-deadletters` to replay them. See [failure analysis](docs/failure-analysis.md).
- For broken pipes or HTTP 413 errors, reduce `bulk.size` (for example, to 200–500), inspect Elasticsearch health and `http.max_content_length`, and consider increasing `realtime.esBulkTimeoutMs`. Pending overflow is flushed in batches.
- For frequent pending overflow, consider increasing `bulk.concurrent` or `maxPending`, reducing `bulk.size`, and improving SQL performance. More pending entries require more memory.
- If MySQL has purged the saved GTID, resetting the position skips intermediate changes. Estimate the missing interval and use bootstrap to recompute it; increase binlog retention and monitor lag. See [troubleshooting](docs/troubleshooting.md).
- Positions are persisted after successful transaction writes and periodically using `realtime.positionSaveIntervalMs`. `position.useGTID` selects GTID rather than file position.
- Set `charset=utf8mb4` and `parseTime=true` in the DSN and keep application and database time zones aligned.
- Use `logging.level=info` or `warn` in production. Enable `debug.sql` and `debug.sqlParams` only when needed.

To avoid HTTP port conflicts when running several modes, set `server.disableForShortTasks: true`, configure `server.realtimePort`, `server.bootstrapPort`, and `server.replayDeadLettersPort`, or override the port with `--server.port`. The precedence is command-line override, mode-specific port, common port, then the default.
