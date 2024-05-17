package raft

type Config struct {
	// Election timeout in milliseconds
	ElectionTimeoutMin int
	ElectionTimeoutMax int

	// How often a Leader should send empty Append Entry heartbeats
	HeartbeatInterval int

	// How often RPCs should be retried
	RPCRetryInterval int
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150,
		ElectionTimeoutMax: 300,
		HeartbeatInterval:  75,
		RPCRetryInterval:   75,
	}
}

// TODO
func FromConfigFile(path string) Config {
	return Config{}
}
