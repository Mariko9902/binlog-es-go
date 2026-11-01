# Failure tracking and analysis

## Failure records

Dead-letter records are stored in `logs/dead-letters/<task>.log` as `timestamp|[id1 id2 id3]|reason`. Application logs provide additional context; check `logs/app.log` or `journalctl -u binlog-es-go`.

```text
2025-10-08T18:02:26+0800|[123 456 789]|es bulk failed: elastic: Error 413 (Request Entity Too Large)
2025-10-08T18:03:10+0800|[1001 1002]|Post "http://server.elasticsearch:9200/ebay_listing3/_bulk": write: broken pipe
```

## Analyze failures

Run the bundled report for a task:

```bash
./scripts/analyze-failures.sh sheet1
```

The report counts failed batches and IDs, groups failures by type and hour, lists recent and largest failures, saves unique IDs, and suggests recovery actions. The following commands provide additional ways to inspect the logs.

### Count failure types

```bash
grep -c '413' logs/dead-letters/sheet1.log
grep -c 'broken pipe' logs/dead-letters/sheet1.log
grep -c 'query.*failed' logs/dead-letters/sheet1.log
grep -c 'version_conflict' logs/dead-letters/sheet1.log
grep -c 'recompute' logs/dead-letters/sheet1.log
```

### Select a time range and extract IDs

```bash
TIME_START="2025-10-08T18:00:00"
TIME_END="2025-10-09T01:00:00"
awk -F'|' -v start="$TIME_START" -v end="$TIME_END" \
  '$1 >= start && $1 <= end' logs/dead-letters/sheet1.log > /tmp/failures_in_range.log
wc -l /tmp/failures_in_range.log
grep -o '\[[^]]*\]' /tmp/failures_in_range.log | \
  tr -d '[]' | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -n | uniq > /tmp/failed_ids.txt
wc -l /tmp/failed_ids.txt
head -20 /tmp/failed_ids.txt
```

### Group by reason and hour

```bash
awk -F'|' '{print $3}' logs/dead-letters/sheet1.log | \
  sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sort | uniq -c | sort -rn
awk -F'|' '{split($1, dt, "T"); split(dt[2], tm, ":"); count[dt[1] " " tm[1] ":00"]++} END {for (h in count) print h, count[h]}' \
  logs/dead-letters/sheet1.log | sort
```

### Correlate binlog positions

```bash
journalctl -u binlog-es-go --since "2025-10-08 18:00:00" --until "2025-10-09 01:00:00" | \
  grep -E '(flush failed|GetEvent error)' -B 5 | grep -E '(binlog|gtid|position)'
```

Inspect the surrounding application logs for the event type, position, and error. Extract affected IDs from the dead-letter file and query MySQL to inspect the corresponding rows.

## Common failures

- **HTTP 413:** The batch exceeded Elasticsearch's `http.max_content_length`. Reduce `bulk.size` or increase the server limit. All IDs in the failed batch are affected.
- **Broken pipe:** Elasticsearch may have timed out or closed the connection, the network may be unstable, or the cluster may be overloaded. Reduce batch size, adjust timeouts, and inspect cluster health.
- **Purged GTID:** MySQL removed binlogs containing the saved position after the process was unable to progress. Estimate the missing interval from the last successful position and timestamps, then recompute the affected data with bootstrap.

For example, after estimating the missing interval:

```bash
LAST_SUCCESS_TIME="2025-10-08 18:00:00"
CURRENT_TIME="2025-10-09 01:00:00"
./bin/binlog-es-go --mode=bootstrap --task=sheet1 \
  --bootstrap.sql-where="s.update_date >= '$LAST_SUCCESS_TIME' AND s.update_date < '$CURRENT_TIME'"
```

Monitor dead-letter growth, error logs, and Prometheus metrics. For recovery, replay dead letters with `--mode=replay-deadletters` or use bootstrap to recompute the affected IDs or time range.
