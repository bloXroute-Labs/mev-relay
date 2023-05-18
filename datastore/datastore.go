// Package datastore helps storing data, utilizing Redis and Postgres as backends
package datastore

import (
	"context"
	"os"
	"sync"

	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type GetHeaderResponseKey struct {
	Slot           uint64
	ParentHash     string
	ProposerPubkey string
}

type GetPayloadResponseKey struct {
	Slot           uint64
	ProposerPubkey string
	BlockHash      string
}

// Datastore provides a local memory cache and Redis
type Datastore struct {
	log *logrus.Entry

	redis      *redisCache
	localCache *localCache

	// feature flags
	ffDisableBidMemoryCache            bool
	ffDisableBidFullPayloadMemoryCache bool
	ffDisableBidRedisCache             bool
}

type localCache struct {
	knownValidatorsByPubKey map[types.PubkeyHex]uint64
	knownValidatorsByIndex  map[uint64]types.PubkeyHex
	knownValidatorsLock     sync.RWMutex

	getHeaderResponsesLock sync.RWMutex
	getHeaderResponses     map[GetHeaderResponseKey]*common.GetHeaderResponse

	getPayloadResponsesLock sync.RWMutex
	getPayloadResponses     map[GetPayloadResponseKey]*common.GetPayloadResponse
}

func NewDatastore(redisURI, prefix string, connectionPoolLimit int, log *logrus.Entry) (ds *Datastore, err error) {
	// connect to Redis
	log.Infof("Connecting to Redis at %s ...", redisURI)
	redisCache, err := newRedisCache(redisURI, prefix, connectionPoolLimit)
	if err != nil {
		log.WithError(err).Fatalf("failed to connect to Redis at %s", redisURI)
	}

	log.Infof("connected to Redis at %s", redisURI)

	ds = &Datastore{
		log:   log.WithField("module", "datastore"),
		redis: redisCache,
		localCache: &localCache{
			knownValidatorsByPubKey: make(map[types.PubkeyHex]uint64),
			knownValidatorsByIndex:  make(map[uint64]types.PubkeyHex),
			getHeaderResponses:      make(map[GetHeaderResponseKey]*common.GetHeaderResponse),
			getPayloadResponses:     make(map[GetPayloadResponseKey]*common.GetPayloadResponse),
		},
	}

	if os.Getenv("DISABLE_BID_MEMORY_CACHE") == "1" {
		ds.log.Warn("env: DISABLE_BID_MEMORY_CACHE - disabling in-memory bid cache")
		ds.ffDisableBidMemoryCache = true
	}

	if os.Getenv("DISABLE_BID_FULL_PAYLOAD_MEMORY_CACHE") == "1" {
		ds.log.Warn("env: DISABLE_BID_FULL_PAYLOAD_MEMORY_CACHE - disabling in-memory bid full payload cache")
		ds.ffDisableBidFullPayloadMemoryCache = true
	}

	if os.Getenv("DISABLE_BID_REDIS_CACHE") == "1" {
		ds.log.Warn("env: DISABLE_BID_REDIS_CACHE - disabling redis bid cache")
		ds.ffDisableBidRedisCache = true
	}

	return ds, err
}

// TxPipeline returns a new transactional pipeline.
func (ds *Datastore) TxPipeline() redis.Pipeliner {
	return ds.redis.client.TxPipeline()
}

func (ds *Datastore) UpdateActiveValidators() error {
	ctx := context.Background()
	return ds.redis.UpdateActiveValidatorCount(ctx)
}
