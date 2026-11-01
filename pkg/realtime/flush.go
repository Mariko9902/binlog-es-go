package realtime

import (
	"context"
	"fmt"
	"strconv"
	"time"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	espkg "github.com/cookchen233/binlog-es-go/pkg/es"
	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/mapper"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/sink"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/transform"
	"github.com/cookchen233/binlog-es-go/pkg/position"
	"go.uber.org/zap"
)

// flush executes the main SQL for keys, transforms rows to docs, and writes to ES (or deletes when configured).
// This was extracted from runner.go to reduce file size and centralize the core pipeline logic.

func (r *Runner) flush(
	ctx context.Context,
	task cfgpkg.SyncTask,
	index string,
	mainSQL string,
	bw *sink.BulkWriter,
	qTimeout time.Duration,
	esTimeout time.Duration,
	lastEventTS *uint32,
	currentFile *string,
	lastPos *uint32,
	lastGTID *string,
	keys []int64,
) error {
	if len(keys) == 0 {
		return nil
	}
	keys = DedupInt64(keys)
	
	
	bulkSize := task.Bulk.Size
	if bulkSize <= 0 {
		bulkSize = 200 
	}
	
	
	if len(keys) > bulkSize {
		r.log.Debug("flush in batches", zap.Int("total_keys", len(keys)), zap.Int("bulk_size", bulkSize), zap.Int("batches", (len(keys)+bulkSize-1)/bulkSize))
		for i := 0; i < len(keys); i += bulkSize {
			end := i + bulkSize
			if end > len(keys) {
				end = len(keys)
			}
			batch := keys[i:end]
			if err := r.flushBatch(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, lastEventTS, currentFile, lastPos, lastGTID, batch); err != nil {
				return err
			}
		}
		return nil
	}
	
	
	return r.flushBatch(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, lastEventTS, currentFile, lastPos, lastGTID, keys)
}


func (r *Runner) flushBatch(
	ctx context.Context,
	task cfgpkg.SyncTask,
	index string,
	mainSQL string,
	bw *sink.BulkWriter,
	qTimeout time.Duration,
	esTimeout time.Duration,
	lastEventTS *uint32,
	currentFile *string,
	lastPos *uint32,
	lastGTID *string,
	keys []int64,
) error {
	if len(keys) == 0 {
		return nil
	}
	var rows []map[string]interface{}
	// keys sample
	if len(keys) > 0 {
		sampleN := len(keys)
		if sampleN > 5 {
			sampleN = 5
		}
		r.log.Debug("exec mapping keys", zap.Int("args_total", len(keys)), zap.Any("keys.sample", keys[:sampleN]))
	}
	
	
	groups := groupKeysByShard(keys, task.Mapping.Sharding)
	for shard, shardKeys := range groups {
		if len(shardKeys) == 0 {
			continue
		}
		shardSQL := renderSQLTemplate(mainSQL, shard, task.Mapping.Sharding)
		var qErr error
		var part []map[string]interface{}
		part, _, qErr = mapper.Do(ctx, qTimeout, mapper.RetryConfig{MaxAttempts: task.Retry.MaxAttempts, BackoffMs: task.Retry.BackoffMs}, r.log, func(qctx context.Context) ([]map[string]interface{}, error) {
			
			
			if _, e := r.db.DB.ExecContext(qctx, "SET SESSION group_concat_max_len = 1048576"); e != nil {
				r.log.Debug("failed to set group_concat_max_len (ignored)", zap.Error(e))
			}
			res, err := r.db.QueryMapping(qctx, shardSQL, shardKeys)
			if err != nil {
				metrics.RetryTotal.WithLabelValues("sql").Inc()
			}
			return res, err
		})
		if qErr != nil {
			return qErr
		}
		if len(part) == 0 {
			if task.Mapping.DeleteOnMissing {
				ids := make([]string, 0, len(shardKeys))
				for _, id := range shardKeys {
					ids = append(ids, fmt.Sprintf("%d", id))
				}
				sample := ids
				if len(sample) > 5 {
					sample = sample[:5]
				}
				r.log.Info("main SQL returned no rows, deleting documents (DeleteOnMissing)", zap.String("index", index), zap.Int("ids", len(ids)), zap.Any("ids.sample", sample), zap.Int("shard", shard))
				if _, err := bw.Delete(ctx, esTimeout, index, ids); err != nil {
					return err
				}
				if *lastEventTS != 0 {
					lag := time.Since(time.Unix(int64(*lastEventTS), 0))
					metrics.RealtimeBinlogLagSeconds.Set(lag.Seconds())
				}
			} else {
				r.log.Debug("main SQL returned no rows, deletion disabled (DeleteOnMissing)", zap.String("index", index), zap.Int("keys", len(shardKeys)), zap.Int("shard", shard))
			}
			continue
		}
		rows = append(rows, part...)
	}
	if len(rows) == 0 {
		return nil
	}
	
	docs := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		doc := transform.NormalizeBytesToString(row)
		if len(task.Transforms.JSONDecodeFields) > 0 {
			transform.JSONDecodeFields(doc, task.Transforms.JSONDecodeFields)
		}
		if len(task.Transforms.SplitFields) > 0 {
			for _, rule := range task.Transforms.SplitFields {
				if rule.Field == "" {
					continue
				}
				transform.SplitStringField(doc, rule.Field, rule.Sep, rule.Trim)
			}
		}
		if r.cfg.Debug.SQL {
			preview := transform.PreviewTopN(doc, 5)
			r.log.Debug("doc preview before upsert", zap.Any("preview", preview))
		}
		docs = append(docs, doc)
	}
	metrics.RealtimeFlushBatchSize.Observe(float64(len(docs)))
	if _, err := bw.Upsert(ctx, esTimeout, index, task.Mapping.ID, docs); err != nil {
		if be, ok := err.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
			
			conf := make([]int64, 0, len(be.ConflictedIDs))
			for _, s := range be.ConflictedIDs {
				if v, e := strconv.ParseInt(s, 10, 64); e == nil {
					conf = append(conf, v)
				}
			}
			cGroups := groupKeysByShard(conf, task.Mapping.Sharding)
			for shard, shardKeys := range cGroups {
				if len(shardKeys) == 0 {
					continue
				}
				ids := make([]string, 0, len(shardKeys))
				for _, id := range shardKeys {
					ids = append(ids, fmt.Sprintf("%d", id))
				}
				shardSQL := renderSQLTemplate(mainSQL, shard, task.Mapping.Sharding)
				_, _ = bw.HandleConflictIDs(
					ctx,
					ids,
					qTimeout,
					esTimeout,
					index,
					task.Mapping.ID,
					shardSQL,
					mapper.RetryConfig{MaxAttempts: task.Retry.MaxAttempts, BackoffMs: task.Retry.BackoffMs},
					r.db.QueryMapping,
					task.Transforms.SplitFields,
					task.Transforms.JSONDecodeFields,
					"realtime",
				)
			}
		} else {
			return err
		}
	}
	r.log.Debug("upsert completed", zap.String("index", index), zap.Int("docs", len(docs)))
	if *lastEventTS != 0 {
		lag := time.Since(time.Unix(int64(*lastEventTS), 0))
		metrics.RealtimeBinlogLagSeconds.Set(lag.Seconds())
	}
	
	if r.cfg.Position.UseGTID && r.cfg.DataSource.GTID {
		if *lastGTID != "" {
			_ = r.posStore.Save(position.State{GTID: *lastGTID})
			r.log.Debug("saved gtid position", zap.String("gtid", *lastGTID))
		} else {
			var cur string
			if err := r.db.DB.Get(&cur, "SELECT @@global.gtid_executed"); err == nil {
				_ = r.posStore.Save(position.State{GTID: cur})
				r.log.Debug("saved gtid position(fallback)", zap.String("gtid_executed", cur))
			} else {
				r.log.Warn("read gtid_executed failed", zap.Error(err))
			}
		}
	} else {
		if err := r.posStore.Save(position.State{File: *currentFile, Pos: *lastPos}); err != nil {
			r.log.Warn("save file/pos failed", zap.Error(err), zap.String("file", *currentFile), zap.Uint32("pos", *lastPos))
		} else {
			r.log.Debug("saved file/pos", zap.String("file", *currentFile), zap.Uint32("pos", *lastPos))
		}
	}
	r.log.Info("realtime synced", zap.Int("docs", len(docs)))
	return nil
}
