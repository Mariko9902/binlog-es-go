package config

// Config is the root config loaded from YAML via viper.
type Config struct {
	DataSource DataSource `mapstructure:"dataSource"`
	ES         ES         `mapstructure:"es"`
	Position   Position   `mapstructure:"position"`
	Bootstrap  Bootstrap  `mapstructure:"bootstrap"`
	Logging    Logging    `mapstructure:"logging"`
	Debug      Debug      `mapstructure:"debug"`
	Server     Server     `mapstructure:"server"`
	Realtime   Realtime   `mapstructure:"realtime"`
	SyncTasks  []SyncTask `mapstructure:"syncTasks"`
}

type DataSource struct {
	DSN      string `mapstructure:"dsn"`
	ServerID int    `mapstructure:"serverId"`
	GTID     bool   `mapstructure:"gtidEnabled"`
	Flavor   string `mapstructure:"flavor"`
}

type ES struct {
	Addresses []string `mapstructure:"addresses"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
	Version   int      `mapstructure:"version"`
	Refresh   string   `mapstructure:"refresh"` // "", "false", "wait_for", "true"
	TLS       ESTLS    `mapstructure:"tls"`
}

type ESTLS struct {
	InsecureSkipVerify bool   `mapstructure:"insecureSkipVerify"`
	CAFile             string `mapstructure:"caFile"`
	CertFile           string `mapstructure:"certFile"`
	KeyFile            string `mapstructure:"keyFile"`
	ServerName         string `mapstructure:"serverName"`
}

type Position struct {
	Path    string `mapstructure:"path"`
	UseGTID bool   `mapstructure:"useGTID"`
}

// Bootstrap default parameters when CLI flags are omitted (zero values)
type Bootstrap struct {
	PartitionSize int `mapstructure:"partitionSize"`
	Workers       int `mapstructure:"workers"`
	BulkSize      int `mapstructure:"bulkSize"`
	BulkFlushMs   int `mapstructure:"bulkFlushMs"`
	RunBatchSize  int `mapstructure:"runBatchSize"` 
}

type Logging struct {
	Level string `mapstructure:"level"` // debug, info, warn, error
	File  string `mapstructure:"file"`  // optional file path, e.g. logs/app.log
}

// Debug controls additional verbose outputs useful during development/testing.
type Debug struct {
	SQL       bool `mapstructure:"sql"`       // print final SQL shape
	SQLParams bool `mapstructure:"sqlParams"` // whether to print SQL param values (consider masking)
}

// Server config for embedded HTTP endpoints (/healthz, /metrics)
type Server struct {
	Port int `mapstructure:"port"` 
	
	RealtimePort          int  `mapstructure:"realtimePort"`          
	BootstrapPort         int  `mapstructure:"bootstrapPort"`         
	ReplayDeadLettersPort int  `mapstructure:"replayDeadLettersPort"` 
	DisableForShortTasks  bool `mapstructure:"disableForShortTasks"`  
}

// Realtime config for binlog reader behavior
type Realtime struct {
	HeartbeatMs   int `mapstructure:"heartbeatMs"`   
	ReadTimeoutMs int `mapstructure:"readTimeoutMs"` 
	MaxPending    int `mapstructure:"maxPending"`    
	
	QueryTimeoutMs         int `mapstructure:"queryTimeoutMs"`         
	ESBulkTimeoutMs        int `mapstructure:"esBulkTimeoutMs"`        
	ESCircuitMaxBackoffMs  int `mapstructure:"esCircuitMaxBackoffMs"`  
	RelatedQueryMaxPages   int `mapstructure:"relatedQueryMaxPages"`   
	PositionSaveIntervalMs int `mapstructure:"positionSaveIntervalMs"` 
}

type SyncTask struct {
	Destination string  `mapstructure:"destination"`
	Mapping     Mapping `mapstructure:"mapping"`
	// Optional: rewrite physical table names from binlog events to logical names for routing.
	// Example: enterprise_00~63 -> enterprise
	TableRewrite []TableRewriteRule `mapstructure:"tableRewrite"`
	// Optional tune settings
	Bulk  Bulk  `mapstructure:"bulk"`
	Retry Retry `mapstructure:"retry"`
	// Table routing and reverse lookup live at task level per YAML
	MappingTable map[string][]string         `mapstructure:"mappingTable"`
	RelatedQuery map[string]RelatedQueryRule `mapstructure:"relatedQuery"`
	// Optional transforms for documents before writing to ES
	Transforms Transforms `mapstructure:"transforms"`
}

// TableRewriteRule rewrites incoming binlog table names for routing purposes.
// Pattern is a regexp (case-insensitive is recommended) matched against the physical table name.
// Replace is the replacement string.
type TableRewriteRule struct {
	Pattern string `mapstructure:"pattern"`
	Replace string `mapstructure:"replace"`
}

type Mapping struct {
	Index  string `mapstructure:"_index"`
	ID     string `mapstructure:"_id"`
	Upsert bool   `mapstructure:"upsert"`
	SQL    string `mapstructure:"sql"`
	// Optional: explicitly declare logical main table name used for binlog routing.
	// Useful when SQL contains template variables (e.g. FROM {{enterprise_table}}).
	MainTable string `mapstructure:"mainTable"`
	// Optional sharding config for main SQL execution. When enabled, keys are grouped by shard,
	// and SQL template variables are rendered per shard (e.g. {{enterprise_table}} -> enterprise_00).
	Sharding *ShardingConfig `mapstructure:"sharding"`
	// When true, if main SQL returns no rows for incoming keys, delete those IDs from ES
	DeleteOnMissing bool `mapstructure:"deleteOnMissing"`
	// When true, on main table delete binlog events, delete corresponding IDs from ES directly
	DeleteOnDelete bool `mapstructure:"deleteOnDelete"`
}

// ShardingConfig describes how to route keys to physical shard tables.
// Vars maps template variable name -> base table name. For each batch shard, variables will be
// rendered as <base>_<suffix> (suffix is zero padded).
// Example: vars: { enterprise_table: enterprise, enterprise_event_table: enterprise_event }
type ShardingConfig struct {
	// Strategy decides how to compute shard for a key.
	// - "crc32_ieee_uint32" (default): crc32_ieee(utf8(string(key))) as uint32, then % shards
	// - "mod": abs(key) % shards
	// - "crc32_ieee_signed_abs": abs(int32(crc32_ieee(bytes))) % shards (compatibility mode)
	// - "crc32": alias of "crc32_ieee_signed_abs" (backward compatibility)
	Strategy    string            `mapstructure:"strategy"`
	Shards      int               `mapstructure:"shards"`
	SuffixWidth int               `mapstructure:"suffixWidth"`
	Vars        map[string]string `mapstructure:"vars"`
}

type RelatedQueryRule struct {
	SQL      string            `mapstructure:"sql"`
	Bind     map[string]string `mapstructure:"bind"`
	PageSize int               `mapstructure:"pageSize"` 
	
	// SELECT AutoID FROM <TargetTable|<main_table>> WHERE <Filter> AND <OrderKey> > :cursor ORDER BY <OrderKey> ASC LIMIT :page_size
	Mode        string `mapstructure:"mode"`        
	TargetTable string `mapstructure:"targetTable"` 
	Filter      string `mapstructure:"filter"`      
	OrderKey    string `mapstructure:"orderKey"`    
}

// Transforms defines optional doc transformations prior to ES upsert
type Transforms struct {
	SplitFields      []SplitFieldRule `mapstructure:"splitFields"`
	JSONDecodeFields []string         `mapstructure:"jsonDecodeFields"`
}

// SplitFieldRule splits a string field into an array by separator
type SplitFieldRule struct {
	Field string `mapstructure:"field"`
	Sep   string `mapstructure:"sep"`  // default ";"
	Trim  bool   `mapstructure:"trim"` // default true
}

type Bulk struct {
	Size            int `mapstructure:"size"`
	FlushIntervalMs int `mapstructure:"flushIntervalMs"`
	Concurrent      int `mapstructure:"concurrent"`
}

type Retry struct {
	MaxAttempts int   `mapstructure:"maxAttempts"`
	BackoffMs   []int `mapstructure:"backoffMs"`
}
