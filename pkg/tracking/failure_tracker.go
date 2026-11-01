package tracking

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)


type FailureRecord struct {
	Timestamp     string   `json:"timestamp"`      
	IDs           []int64  `json:"ids"`            
	IDCount       int      `json:"id_count"`       
	FailureType   string   `json:"failure_type"`   
	Reason        string   `json:"reason"`         
	Stage         string   `json:"stage"`          
	BinlogFile    string   `json:"binlog_file"`    
	BinlogPos     uint32   `json:"binlog_pos"`     
	GTID          string   `json:"gtid"`           
	EventType     string   `json:"event_type"`     
	SourceTable   string   `json:"source_table"`   
	RetryAttempts int      `json:"retry_attempts"` 
	Context       string   `json:"context"`        
}


type FailureTracker struct {
	task      string
	log       *zap.Logger
	mu        sync.Mutex
	file      *os.File
	jsonFile  *os.File 
}


func NewFailureTracker(task string, log *zap.Logger) (*FailureTracker, error) {
	dir := filepath.Join("logs", "failure-tracking")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	
	textPath := filepath.Join(dir, fmt.Sprintf("%s.log", task))
	textFile, err := os.OpenFile(textPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	
	jsonPath := filepath.Join(dir, fmt.Sprintf("%s.jsonl", task))
	jsonFile, err := os.OpenFile(jsonPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		textFile.Close()
		return nil, err
	}

	return &FailureTracker{
		task:     task,
		log:      log,
		file:     textFile,
		jsonFile: jsonFile,
	}, nil
}


func (ft *FailureTracker) RecordFailure(record FailureRecord) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	record.Timestamp = time.Now().Format(time.RFC3339)
	record.IDCount = len(record.IDs)

	
	jsonData, err := json.Marshal(record)
	if err != nil {
		ft.log.Warn("marshal failure record failed", zap.Error(err))
		return
	}
	ft.jsonFile.Write(jsonData)
	ft.jsonFile.Write([]byte("\n"))

	
	
	textLine := fmt.Sprintf("%s|%s|%s|%d IDs|%s|binlog:%s@%d|gtid:%s|%s\n",
		record.Timestamp,
		record.FailureType,
		record.Stage,
		record.IDCount,
		truncateString(record.Reason, 100),
		record.BinlogFile,
		record.BinlogPos,
		truncateString(record.GTID, 50),
		record.Context,
	)
	ft.file.WriteString(textLine)

	
	if len(record.IDs) <= 100 {
		idLine := fmt.Sprintf("  └─ IDs: %v\n", record.IDs)
		ft.file.WriteString(idLine)
	} else {
		
		sample := record.IDs[:10]
		idLine := fmt.Sprintf("  └─ IDs (sample): %v ... (total: %d)\n", sample, len(record.IDs))
		ft.file.WriteString(idLine)
	}

	ft.log.Warn("record failure",
		zap.String("type", record.FailureType),
		zap.Int("ids", len(record.IDs)),
		zap.String("reason", truncateString(record.Reason, 100)),
	)
}


func (ft *FailureTracker) Close() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	if ft.file != nil {
		ft.file.Close()
	}
	if ft.jsonFile != nil {
		ft.jsonFile.Close()
	}
	return nil
}


func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}


func AnalyzeFailures(task string) (map[string]int, error) {
	jsonPath := filepath.Join("logs", "failure-tracking", fmt.Sprintf("%s.jsonl", task))
	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stats := make(map[string]int)
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var record FailureRecord
		if err := decoder.Decode(&record); err != nil {
			continue
		}
		stats[record.FailureType] += record.IDCount
	}

	return stats, nil
}
