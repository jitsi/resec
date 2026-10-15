package redis

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/jpillora/backoff"
	"github.com/seatgeek/resec/resec/state"
	log "github.com/sirupsen/logrus"
)

type Manager struct {
	backoff        *backoff.Backoff // exponential backoff helper
	logger         *log.Entry       // logging specificall for Redis
	client         *redis.Client    // redis client
	config         *Config          // redis config
	state          *state.Redis     // redis state
	stateCh        chan state.Redis // redis state channel to publish updates to the reconciler
	commandCh      chan Command
	stopCh         chan interface{}
	watcherRunning bool
	linkDownSince  time.Time // when we first saw the master link down
	linkDownMaster string    // master (host:port) the link was down to
}

type Config struct {
	Address string // address (IP+Port) used to talk to Redis
}
