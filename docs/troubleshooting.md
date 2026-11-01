# Troubleshooting

| Problem | Severity | Log indicators | Effect |
| --- | --- | --- | --- |
| Elasticsearch write failure | High | `broken pipe`, `es bulk failed` | Backlog and possible data loss |
| Pending overflow | Medium | `pending overflow` | Memory pressure and forced flush |
| Circuit breaker open | Medium | `circuit open, sleep` | Paused writes and increased latency |
| GTID position purged | Critical | `ERROR 1236`, `purged binary logs` | Synchronization cannot continue |
| Version conflict | Low | `version_conflict` | Automatic recomputation |

## Elasticsearch write failures

A large batch can exceed `http.max_content_length`, time out, or increase cluster memory pressure. Network instability and high Elasticsearch load can also produce broken pipes. Repeated failures open the circuit breaker and can cause pending entries to accumulate.

```yaml
syncTasks:
  - bulk:
      size: 200
      flushIntervalMs: 500
      concurrent: 8
realtime:
  esBulkTimeoutMs: 120000
  maxPending: 50000
```

Adjust the values to match the workload. The program splits forced flushes into batches using `bulk.size`. Inspect Elasticsearch before changing its limits:

```bash
curl http://server.elasticsearch:9200/_cluster/health?pretty
curl http://server.elasticsearch:9200/ebay_listing3/_stats?pretty
curl http://server.elasticsearch:9200/_nodes/stats/indices/search?pretty
```

If necessary, adjust `http.max_content_length` in Elasticsearch configuration.

## Pending overflow

Pending overflow occurs when binlog changes arrive faster than Elasticsearch can process them or writes fail. Occasional forced flushes can be normal; frequent overflow calls for investigation. Improve Elasticsearch throughput, reduce batch size, increase concurrency where appropriate, or optimize the main SQL and indexes. Increasing `realtime.maxPending` gives the process more buffering at the cost of memory.

```sql
EXPLAIN SELECT ... FROM sheet1 s ... WHERE s.AutoID IN (...);
CREATE INDEX idx_autoid ON sheet1(AutoID);
CREATE INDEX idx_category_autoid ON sheet1(Category_id, AutoID);
```

## Circuit breaker

Consecutive Elasticsearch failures open the circuit and pause writes. The retry delay grows exponentially up to `realtime.esCircuitMaxBackoffMs`. Frequent activation indicates that Elasticsearch or its connection needs attention.

```yaml
realtime:
  esCircuitMaxBackoffMs: 60000
```

## Purged GTID position

MySQL may purge the saved GTID after an extended outage. Resetting the position to the current GTID allows synchronization to resume but skips intermediate changes. Estimate the missing interval and recompute it with bootstrap.

```bash
kill <pid>
mysql -hserver.mysql -uroot -p123456 -e "SELECT @@global.gtid_executed;"
cat > data/position.json <<EOF_POSITION
{"gtid": "3e11fa47-5729-11e6-9c6c-42010a8000a6:1-123456"}
EOF_POSITION
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=sheet1 \
  --bootstrap.sql-where="s.update_date >= '2025-10-02' AND s.update_date < '2025-10-08'"
./bin/binlog-es-go --config=configs/config.yaml --mode=realtime --task=sheet1
```

Increase binlog retention as appropriate and monitor process health, `binlog_es_sync_reconnect_total`, and binlog lag:

```sql
SET GLOBAL binlog_expire_logs_seconds = 604800;
SET GLOBAL expire_logs_days = 7;
SHOW BINARY LOGS;
SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 1;
```

The first retention setting applies to MySQL 8.0 and the second to MySQL 5.7. Persist the appropriate setting in `my.cnf` if required.

## Monitoring and log analysis

```bash
curl http://localhost:8222/metrics | grep binlog_es_sync
zgrep -h '"L":"ERROR"' logs/app-*.log.gz | jq -r '.M' | sort | uniq -c | sort -rn
zgrep -h '"L":"WARN"' logs/app-*.log.gz | jq -r '.M' | sort | uniq -c | sort -rn
zgrep 'broken pipe' logs/app-*.log.gz | wc -l
grep 'pending overflow' logs/app.log
grep 'circuit open' logs/app.log
grep 'ERROR 1236' logs/app.log
grep 'realtime synced' logs/app.log
```

Useful metrics include `binlog_es_sync_realtime_binlog_lag_seconds`, `binlog_es_sync_retry_total`, and `binlog_es_sync_dead_letters_total`. See [failure analysis](failure-analysis.md) for dead-letter inspection and recovery.
