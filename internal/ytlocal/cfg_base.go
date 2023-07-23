package ytlocal

// AddressResolver config.
type AddressResolver struct {
	Retries       int    `yson:"retries,omitempty"`
	EnableIPv6    bool   `yson:"enable_ipv6"`
	EnableIPv4    bool   `yson:"enable_ipv4"`
	LocalhostFQDN string `yson:"localhost_fqdn,omitempty"`
}

// Connection config.
type Connection struct {
	Addresses                  []string `yson:"addresses"`
	EnableMasterCacheDiscovery bool     `yson:"enable_master_cache_discovery"`
	CellID                     string   `yson:"cell_id,omitempty"`
	SoftBackoffTime            int      `yson:"soft_backoff_time,omitempty"`
	HardBackoffTime            int      `yson:"hard_backoff_time,omitempty"`
	RetryBackoffTime           int      `yson:"retry_backoff_time,omitempty"`
	RetryAttempts              int      `yson:"retry_attempts,omitempty"`
	EnablePeerPolling          bool     `yson:"enable_peer_polling,omitempty"`
	PeerPollingPeriod          int      `yson:"peer_polling_period,omitempty"`
	PeerPollingPeriodSplay     int      `yson:"peer_polling_period_splay,omitempty"`
	PeerPollingRequestTimeout  int      `yson:"peer_polling_request_timeout,omitempty"`
	RediscoverPeriod           int      `yson:"rediscover_period,omitempty"`
	RediscoverSplay            int      `yson:"rediscover_splay,omitempty"`
	RPCTimeout                 int      `yson:"rpc_timeout,omitempty"`
	UpdatePeriod               int      `yson:"update_period,omitempty"`
}

// CellDirectory config.
type CellDirectory struct {
	SoftBackoffTime           int  `yson:"soft_backoff_time,omitempty"`
	HardBackoffTime           int  `yson:"hard_backoff_time,omitempty"`
	RetryBackoffTime          int  `yson:"retry_backoff_time,omitempty"`
	EnablePeerPolling         bool `yson:"enable_peer_polling,omitempty"`
	PeerPollingPeriod         int  `yson:"peer_polling_period,omitempty"`
	PeerPollingPeriodSplay    int  `yson:"peer_polling_period_splay,omitempty"`
	PeerPollingRequestTimeout int  `yson:"peer_polling_request_timeout,omitempty"`
	RediscoverPeriod          int  `yson:"rediscover_period,omitempty"`
	RediscoverSplay           int  `yson:"rediscover_splay,omitempty"`
}

// TransactionManager config.
type TransactionManager struct {
	DefaultPingPeriod int `yson:"default_ping_period"`
}

// ClusterConnection config.
type ClusterConnection struct {
	ClusterName         string             `yson:"cluster_name"`
	DiscoveryConnection Connection         `yson:"discovery_connection"`
	PrimaryMaster       Connection         `yson:"primary_master"`
	CellDirectory       CellDirectory      `yson:"cell_directory"`
	TransactionManager  TransactionManager `yson:"transaction_manager"`
	TimestampProvider   Connection         `yson:"timestamp_provider"`
}

// BaseServer wraps common server configiguratio parts.
type BaseServer struct {
	RPCPort           int               `yson:"rpc_port"`
	MonitoringPort    int               `yson:"monitoring_port"`
	Logging           Logging           `yson:"logging"`
	AddressResolver   AddressResolver   `yson:"address_resolver"`
	TimestampProvider Connection        `yson:"timestamp_provider"`
	ClusterConnection ClusterConnection `yson:"cluster_connection"`
	SkynetHTTPPort    int               `yson:"skynet_http_port,omitempty"`
}
