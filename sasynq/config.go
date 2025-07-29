package sasynq

import (
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

// RedisMode defines the Redis connection mode.
type RedisMode string

const (
	// RedisModeSingle uses a single Redis instance.
	RedisModeSingle RedisMode = "single"
	// RedisModeSentinel uses Redis Sentinel for high availability.
	RedisModeSentinel RedisMode = "sentinel"
	// RedisModeCluster uses a Redis Cluster for horizontal scaling.
	RedisModeCluster RedisMode = "cluster"
)

// RedisConfig holds all configurations for connecting to Redis.
type RedisConfig struct {
	Mode RedisMode `yaml:"mode"`

	// For Single Mode
	Addr string `yaml:"addr"`

	// For Sentinel Mode
	SentinelAddrs []string `yaml:"sentinelAddrs"`
	MasterName    string   `yaml:"masterName"`

	// For Cluster Mode
	ClusterAddrs []string `yaml:"clusterAddrs"`

	// Common options
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// GetAsynqRedisConnOpt converts RedisConfig to asynq's RedisConnOpt interface.
// This is the core of the high-availability switching logic.
func (c *RedisConfig) GetAsynqRedisConnOpt() asynq.RedisConnOpt {
	switch c.Mode {
	case RedisModeSentinel:
		return asynq.RedisFailoverClientOpt{
			MasterName:    c.MasterName,
			SentinelAddrs: c.SentinelAddrs,
			Username:      c.Username,
			Password:      c.Password,
			DB:            c.DB,
		}
	case RedisModeCluster:
		return asynq.RedisClusterClientOpt{
			Addrs:    c.ClusterAddrs,
			Username: c.Username,
			Password: c.Password,
		}
	case RedisModeSingle:
		fallthrough
	default:
		return asynq.RedisClientOpt{
			Addr:     c.Addr,
			Username: c.Username,
			Password: c.Password,
			DB:       c.DB,
		}
	}
}

// ServerConfig holds configurations for the asynq server.
type ServerConfig struct {
	*asynq.Config
}

// DefaultServerConfig returns a default server configuration.
func DefaultServerConfig(l ...*zap.Logger) ServerConfig {
	var zapLogger *zap.Logger
	if len(l) == 0 {
		zapLogger = defaultLogger
	} else {
		zapLogger = l[0]
	}

	cfg := &asynq.Config{
		Concurrency: 10,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
		Logger: NewZapLogger(zapLogger),
	}

	return ServerConfig{cfg}
}
