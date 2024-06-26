package raft

type Config struct {
	// Election timeout in milliseconds
	ElectionTimeoutMin int
	ElectionTimeoutMax int

	// How often a Leader should send empty Append Entry heartbeats
	HeartbeatInterval int

	// Heartbeat batch size
	MaximumBatchSize int

	// How often RPCs should be retried
	RPCRetryInterval             int
	ForwardToLeaderRetryInterval int

	// Time to wait for log being applied before replying to client
	UpdateTimeout int
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin:           150,
		ElectionTimeoutMax:           300,
		HeartbeatInterval:            75,
		MaximumBatchSize:             10,
		RPCRetryInterval:             75,
		ForwardToLeaderRetryInterval: 150000,
		UpdateTimeout:                1500,
	}
}

// TODO
func FromConfigFile(path string) Config {
	return Config{}
}
