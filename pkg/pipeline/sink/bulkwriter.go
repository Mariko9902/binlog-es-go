package sink

import (
	"context"
	"fmt"
	"time"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	espkg "github.com/cookchen233/binlog-es-go/pkg/es"
	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/mapper"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/transform"
	"github.com/cookchen233/binlog-es-go/pkg/util"
	"go.uber.org/zap"
)

// RetryConfig mirrors mapper retry.
type RetryConfig struct {
	MaxAttempts int
	BackoffMs   []int
}

// Options configures BulkWriter behavior.
type Options struct {
	Retry          RetryConfig
	CircuitEnabled bool
	MaxBackoff     time.Duration // max backoff used when circuit opens
}

// BulkWriter wraps ES writer with retry, metrics and optional circuit breaker.
type BulkWriter struct {
	es   *espkg.Writer
	log  *zap.Logger
	opts Options

	// circuit state
	consecFail int
	cbUntil    time.Time
}

func NewBulkWriter(es *espkg.Writer, log *zap.Logger, opts Options) *BulkWriter {
	bw := &BulkWriter{es: es, log: log, opts: opts}
	if bw.opts.MaxBackoff <= 0 {
		bw.opts.MaxBackoff = 30 * time.Second
	}
	return bw
}

// waitIfCircuitOpen sleeps until circuit closes or ctx done.
func (b *BulkWriter) waitIfCircuitOpen(ctx context.Context) error {
	if !b.opts.CircuitEnabled {
		return nil
	}
	if time.Now().Before(b.cbUntil) {
		sleep := time.Until(b.cbUntil)
		b.log.Warn("es circuit open, sleep", zap.Duration("sleep", sleep))
		select {
		case <-time.After(sleep):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *BulkWriter) onSuccess(kind string, n int, elapsed time.Duration) {
	// reset circuit
	b.consecFail = 0
	b.cbUntil = time.Time{}
	// metrics
	if kind == "upsert" {
		if n > 0 {
			metrics.UpsertSuccessTotal.Add(float64(n))
		}
	} else if kind == "delete" {
		if n > 0 {
			metrics.DeleteSuccessTotal.WithLabelValues("delete").Add(float64(n))
		}
	}
	metrics.ESBulkLatencySeconds.Observe(elapsed.Seconds())
}

func (b *BulkWriter) onFailure(msg string, err error) {
	// backoff & open circuit
	if !b.opts.CircuitEnabled {
		return
	}
	b.consecFail++
	backoff := time.Second << (b.consecFail - 1)
	if backoff > b.opts.MaxBackoff {
		backoff = b.opts.MaxBackoff
	}
	b.cbUntil = time.Now().Add(backoff)
	b.log.Warn(msg, zap.Int("consecutive_fail", b.consecFail), zap.Duration("backoff", backoff), zap.Error(err))
}

// Upsert performs bulk upsert with retry, metrics, and optional circuit breaker.
func (b *BulkWriter) Upsert(ctx context.Context, timeout time.Duration, index, idField string, docs []map[string]interface{}) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	if err := b.waitIfCircuitOpen(ctx); err != nil {
		return 0, err
	}
	var affected int
	var elapsed time.Duration
	err := util.Retry(b.opts.Retry.MaxAttempts, b.opts.Retry.BackoffMs, func() error {
		ctx2, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		start := time.Now()
		n, e := b.es.UpsertBulk(ctx2, index, idField, docs)
		elapsed = time.Since(start)
		affected = n
		if e != nil {
			
			if be, ok := e.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
				b.log.Warn("upsert version conflict, recomputation required", zap.Any("conflicted", be.ConflictedIDs))
				return e
			}
			metrics.RetryTotal.WithLabelValues("es").Inc()
			b.log.Warn("es bulk upsert retry", zap.Error(e), zap.Int("docs", len(docs)))
		}
		return e
	}, nil)
	if err != nil {
		b.onFailure("es bulk failed, open circuit", err)
		return 0, err
	}
	b.onSuccess("upsert", affected, elapsed)
	return affected, nil
}

// Delete performs bulk delete with retry, metrics, and optional circuit breaker.
func (b *BulkWriter) Delete(ctx context.Context, timeout time.Duration, index string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if err := b.waitIfCircuitOpen(ctx); err != nil {
		return 0, err
	}
	var affected int
	var elapsed time.Duration
	err := util.Retry(b.opts.Retry.MaxAttempts, b.opts.Retry.BackoffMs, func() error {
		ctx2, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		start := time.Now()
		n, e := b.es.DeleteBulk(ctx2, index, ids)
		elapsed = time.Since(start)
		affected = n
		if e != nil {
			if be, ok := e.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
				b.log.Warn("delete version conflict, recomputation required", zap.Any("conflicted", be.ConflictedIDs))
				return e
			}
			metrics.RetryTotal.WithLabelValues("es").Inc()
			b.log.Warn("es bulk delete retry", zap.Error(e), zap.Int("ids", len(ids)))
		}
		return e
	}, nil)
	if err != nil {
		b.onFailure("es delete failed, open circuit", err)
		return 0, err
	}
	b.onSuccess("delete", affected, elapsed)
	return affected, nil
}















//




func (b *BulkWriter) HandleConflictIDs(
	ctx context.Context,
	conflictedIDs []string,
	qTimeout time.Duration,
	esTimeout time.Duration,
	index string,
	idField string,
	mainSQL string,
	retryCfg mapper.RetryConfig,
	queryFn func(context.Context, string, []int64) ([]map[string]interface{}, error),
	transformRules []cfgpkg.SplitFieldRule,
	jsonDecodeFields []string,
	logContext string,
) (int, error) {
	if len(conflictedIDs) == 0 {
		return 0, nil
	}

	
	ids2 := make([]int64, 0, len(conflictedIDs))
	for _, s := range conflictedIDs {
		var v int64
		fmt.Sscanf(s, "%d", &v)
		if v > 0 {
			ids2 = append(ids2, v)
		}
	}

	b.log.Warn("version conflict, recomputing affected IDs",
		zap.String("context", logContext),
		zap.Any("conflicted", conflictedIDs))

	
	var rows2 []map[string]interface{}
	rows2, _, _ = mapper.Do(ctx, qTimeout, retryCfg, b.log, func(qctx context.Context) ([]map[string]interface{}, error) {
		return queryFn(qctx, mainSQL, ids2)
	})

	if len(rows2) == 0 {
		b.log.Warn("recomputation returned no rows for conflicted IDs",
			zap.String("context", logContext),
			zap.Int("ids", len(ids2)))
		
		b.log.Error("record dead letter", zap.String("context", logContext), zap.Any("ids", ids2), zap.String("reason", "recompute returned no rows for conflicted ids"))
		return 0, nil
	}

	
	docs2 := make([]map[string]interface{}, 0, len(rows2))
	for _, row := range rows2 {
		doc := transform.NormalizeBytesToString(row)
		if len(jsonDecodeFields) > 0 {
			transform.JSONDecodeFields(doc, jsonDecodeFields)
		}
		if len(transformRules) > 0 {
			for _, rule := range transformRules {
				if rule.Field == "" {
					continue
				}
				transform.SplitStringField(doc, rule.Field, rule.Sep, rule.Trim)
			}
		}
		docs2 = append(docs2, doc)
	}

	
	affected, err := b.Upsert(ctx, esTimeout, index, idField, docs2)
	if err != nil {
		b.log.Error("write failed after recomputing conflicted IDs",
			zap.String("context", logContext),
			zap.Error(err),
			zap.Int("docs", len(docs2)))
		
		b.log.Error("record dead letter", zap.String("context", logContext), zap.Any("ids", ids2), zap.String("reason", fmt.Sprintf("es bulk failed after recompute: %v", err)))
		return affected, err
	}

	return affected, nil
}
