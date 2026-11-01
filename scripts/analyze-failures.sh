#!/bin/bash

set -e

TASK=${1:-sheet1}
DEAD_LETTER_FILE="logs/dead-letters/${TASK}.log"

echo "========================================="
echo "  Failure analysis report - Task: $TASK"
echo "  Generated: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="
echo ""

if [ ! -f "$DEAD_LETTER_FILE" ]; then
    echo "✅ No dead-letter file found"
    exit 0
fi

echo "## 1. Overall statistics"
echo "----------------------------------------"
TOTAL_BATCHES=$(wc -l < "$DEAD_LETTER_FILE")
echo "Failed batches: $TOTAL_BATCHES"

TOTAL_IDS=$(grep -o '\[[^]]*\]' "$DEAD_LETTER_FILE" | \
    tr -d '[]' | tr ' ' '\n' | grep -E '^[0-9]+$' | wc -l)
echo "Total failed IDs: $TOTAL_IDS"

if [ $TOTAL_BATCHES -gt 0 ]; then
    AVG_PER_BATCH=$((TOTAL_IDS / TOTAL_BATCHES))
    echo "Average failures per batch: $AVG_PER_BATCH"
fi
echo ""

echo "## 2. Failure types"
echo "----------------------------------------"
awk -F'|' '{
    reason = $3;
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", reason);
    
    if (reason ~ /413|Request Entity Too Large/) {
        type = "ES_413_request_too_large";
    } else if (reason ~ /broken pipe/) {
        type = "network_broken_pipe";
    } else if (reason ~ /version_conflict/) {
        type = "version conflict";
    } else if (reason ~ /query.*failed|sql.*failed/) {
        type = "SQL query failure";
    } else if (reason ~ /recompute.*failed/) {
        type = "recomputation failure";
    } else if (reason ~ /timeout/) {
        type = "timeout";
    } else {
        type = "other";
    }
    
    count[type]++;
}
END {
    for (t in count) {
        printf "%-25s : %d\n", t, count[t];
    }
}' "$DEAD_LETTER_FILE" | sort -t':' -k2 -rn
echo ""

echo "## 3. Failures by hour"
echo "----------------------------------------"
awk -F'|' '{
    split($1, dt, "T");
    date_part = dt[1];
    split(dt[2], tm, ":");
    hour = tm[1];
    time_key = date_part " " hour ":00";
    
    count[time_key]++;
}
END {
    for (t in count) {
        print t, count[t];
    }
}' "$DEAD_LETTER_FILE" | sort | \
    awk '{printf "%-20s : %d\n", $1" "$2, $3}'
echo ""

echo "## 4. Largest failed batches (top 5)"
echo "----------------------------------------"
awk -F'|' '{
    time = $1;
    ids = $2;
    reason = $3;
    
    gsub(/[^0-9 ]/, "", ids);
    split(ids, id_arr, " ");
    count = 0;
    for (i in id_arr) {
        if (id_arr[i] != "") count++;
    }
    
    print count, time, reason;
}' "$DEAD_LETTER_FILE" | sort -rn | head -5 | \
    awk '{
        printf "%s - %d - %s\n", $2, $1, substr($0, index($0,$3));
    }'
echo ""

echo "## 5. Failed IDs"
echo "----------------------------------------"
TMP_IDS="/tmp/failed_ids_${TASK}.txt"
grep -o '\[[^]]*\]' "$DEAD_LETTER_FILE" | \
    tr -d '[]' | tr ' ' '\n' | \
    grep -E '^[0-9]+$' | sort -n | uniq > "$TMP_IDS"

UNIQUE_IDS=$(wc -l < "$TMP_IDS")
echo "Unique failed IDs: $UNIQUE_IDS"
echo "ID range: $(head -1 "$TMP_IDS") ~ $(tail -1 "$TMP_IDS")"
echo "Failed IDs saved to: $TMP_IDS"
echo ""

echo "## 6. Last 5 failures"
echo "----------------------------------------"
tail -5 "$DEAD_LETTER_FILE" | while IFS='|' read -r time ids reason; do
    id_count=$(echo "$ids" | grep -o '[0-9]\+' | wc -l)
    echo "Time: $time"
    echo "Count: $id_count"
    echo "Reason: $reason"
    echo "---"
done
echo ""

echo "## 7. Diagnosis and recommendations"
echo "----------------------------------------"

if grep -q "413\|Request Entity Too Large" "$DEAD_LETTER_FILE"; then
    echo "🔴 ES 413 error (request too large)"
    echo "   Reason: bulk.size is too large, or pending overflow caused an oversized batch"
    echo "   Recommendations: "
    echo "   1. Reduce bulk.size to 200-300"
    echo "   2. Check ES http.max_content_length"
    echo "   3. Ensure batch flushing is enabled"
    echo ""
fi

if grep -q "broken pipe" "$DEAD_LETTER_FILE"; then
    echo "🔴 Broken pipe (connection closed)"
    echo "   Reason: ES timed out or the network is unstable"
    echo "   Recommendations: "
    echo "   1. Reduce bulk.size"
    echo "   2. Increase esBulkTimeoutMs to 120000"
    echo "   3. Check ES cluster health"
    echo ""
fi

if grep -q "version_conflict" "$DEAD_LETTER_FILE"; then
    echo "⚠️  Version conflict detected"
    echo "   Reason: Concurrent writes or duplicate processing"
    echo "   Note: The system recomputes automatically and records a dead letter only if recomputation fails"
    echo ""
fi

if grep -q "query.*failed\|sql.*failed" "$DEAD_LETTER_FILE"; then
    echo "🔴 SQL query failure detected"
    echo "   Reason: Database connection or SQL performance issue"
    echo "   Recommendations: "
    echo "   1. Check the database connection"
    echo "   2. Optimize the main SQL query"
    echo "   3. Add the necessary indexes"
    echo ""
fi

echo "## 8. Recovery recommendations"
echo "----------------------------------------"
echo "Option 1: Replay dead letters (recommended)"
echo "  ./bin/binlog-es-go --mode=replay-deadletters --task=$TASK"
echo ""
echo "Option 2: Recover by ID list"

echo "  cat $TMP_IDS | head -1000 | tr '\\n' ',' | sed 's/,\$//' | xargs -I {} \\"
echo "    ./bin/binlog-es-go --mode=bootstrap --task=$TASK \\"
echo "    --bootstrap.sql-where=\"s.AutoID IN ({})\""
echo ""
echo "Option 3: Recover by time range (if known)"

echo "  START_TIME=\$(head -1 $DEAD_LETTER_FILE | cut -d'|' -f1)"
echo "  END_TIME=\$(tail -1 $DEAD_LETTER_FILE | cut -d'|' -f1)"
echo "  ./bin/binlog-es-go --mode=bootstrap --task=$TASK \\"
echo "    --bootstrap.sql-where=\"s.update_date >= '\$START_TIME' AND s.update_date < '\$END_TIME'\""
echo ""

echo "========================================="
echo "  Analysis complete"
echo "========================================="
