package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/attestantio/go-builder-client/api"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

var (
	redisPrefix = "boost-relay"

	expiryBidCache              = 1 * time.Minute
	expiryBlockSubmissionStatus = 24 * time.Second

	expiryActiveValidators = 3
)

type BlockSimulationStatus string

const (
	BlockSimulationSubmitted = "1"
	BlockSimulationPassed    = "2"
	BlockSimulationFailed    = "3"
)

type redisCache struct {
	client *redis.Client

	prefixGetHeaderResponse  string
	prefixGetPayloadResponse string

	prefixBlockSimulationStatus           string
	prefixBlockBuilderLatestBids          string // latest bid for a given slot
	prefixBlockBuilderLatestBidsValue     string // value of the latest bid for a given slot
	prefixBlockBuilderLatestBidsValueTime string // value of the latest bid for a given slot
	prefixBlockBuilderLatestBidsTime      string // when the request was received, to avoid older requests overwriting newer ones after a slot validation
	prefixDeliveredPayload                string
	prefixBlockBuilderSubmission          string
	prefixBidTrace                        string

	keyDemotedBuilders                string
	keyKnownValidators                string
	keyValidatorRegistration          string
	prefixActiveValidators            string
	prefixActiveValidatorsInt         string
	keyValidatorRegistrationTimestamp string

	keyRelayConfig    string
	keyStats          string
	keyProposerDuties string

	getHeaderList sync.Mutex
}

func newRedisCache(redisURI, prefix string, connectionPoolLimit int) (*redisCache, error) {
	client, err := connectRedis(redisURI, connectionPoolLimit)
	if err != nil {
		return nil, err
	}

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(client); err != nil {
		return nil, err
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(client); err != nil {
		return nil, err
	}

	return &redisCache{
		client: client,

		prefixGetHeaderResponse:  fmt.Sprintf("%s/%s:cache-gethead-response", redisPrefix, prefix),
		prefixGetPayloadResponse: fmt.Sprintf("%s/%s:cache-getpayload-response", redisPrefix, prefix),

		prefixBlockSimulationStatus:           fmt.Sprintf("%s/%s:cache-block-simulation-status", redisPrefix, prefix),
		prefixBlockBuilderLatestBids:          fmt.Sprintf("%s/%s:block-builder-latest-bid", redisPrefix, prefix),            // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsValue:     fmt.Sprintf("%s/%s:block-builder-latest-bid-value", redisPrefix, prefix),      // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsTime:      fmt.Sprintf("%s/%s:block-builder-latest-bid-time", redisPrefix, prefix),       // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixBlockBuilderLatestBidsValueTime: fmt.Sprintf("%s/%s:block-builder-latest-bid-value-time", redisPrefix, prefix), // hashmap for slot+parentHash+proposerPubkey with builderPubkey as field
		prefixDeliveredPayload:                fmt.Sprintf("%s/%s:relay-delivered-payload", redisPrefix, prefix),
		prefixBlockBuilderSubmission:          fmt.Sprintf("%s/%s:builder-block-submission", redisPrefix, prefix),
		prefixBidTrace:                        fmt.Sprintf("%s/%s:bid-trace", redisPrefix, prefix),

		keyDemotedBuilders:                fmt.Sprintf("%s/%s:builder-demoted", redisPrefix, prefix),
		keyKnownValidators:                fmt.Sprintf("%s/%s:known-validators", redisPrefix, prefix),
		keyValidatorRegistration:          fmt.Sprintf("%s/%s:validators-registration-timestamp", redisPrefix, prefix),
		keyValidatorRegistrationTimestamp: fmt.Sprintf("%s/%s:validators-registration", redisPrefix, prefix),
		prefixActiveValidators:            fmt.Sprintf("%s/%s:active-validators", redisPrefix, prefix),
		prefixActiveValidatorsInt:         fmt.Sprintf("%s/%s:active-validators-int", redisPrefix, prefix),
		keyRelayConfig:                    fmt.Sprintf("%s/%s:relay-config", redisPrefix, prefix),

		keyStats:          fmt.Sprintf("%s/%s:stats", redisPrefix, prefix),
		keyProposerDuties: fmt.Sprintf("%s/%s:proposer-duties", redisPrefix, prefix),
	}, nil
}

func connectRedis(redisURI string, connectionPoolLimit int) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		PoolSize:     connectionPoolLimit,
		Addr:         redisURI,
		MinIdleConns: connectionPoolLimit / 2,
		PoolTimeout:  20 * time.Second,
		ReadTimeout:  20 * time.Second,
		WriteTimeout: 15 * time.Second,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		// unable to connect to redis
		return nil, err
	}
	return redisClient, nil
}

func (r *redisCache) getObj(ctx context.Context, key string, obj any) (err error) {
	value, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(value), &obj)
}

func (r *redisCache) getObjSSZ(ctx context.Context, key string) (*common.GetPayloadResponse, error) {

	res := new(common.GetPayloadResponse)
	capData := new(capella.ExecutionPayload)

	value, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	if err := capData.UnmarshalSSZ([]byte(value)); err != nil {
		return nil, err
	}

	res.Capella = new(api.VersionedExecutionPayload)
	res.Capella.Version = consensusspec.DataVersionCapella
	res.Capella.Capella = capData

	return res, nil
}

func (r *redisCache) getString(ctx context.Context, key string) (res string, err error) {
	return r.client.Get(ctx, key).Result()
}

func (r *redisCache) setString(ctx context.Context, key string, value string, expiration time.Duration) (err error) {
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *redisCache) setObj(ctx context.Context, key string, value any, expiration time.Duration) (err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return r.client.Set(ctx, key, marshalledValue, expiration).Err()
}

// setObjTx is a transactional version of setObj
func setObjTx(ctx context.Context, tx redis.Pipeliner, key string, value any, expiration time.Duration) (err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return tx.Set(ctx, key, marshalledValue, expiration).Err()
}

// setObjTx is a transactional version of setObj
func setObjTxWithReturn(ctx context.Context, tx redis.Pipeliner, key string, value any, expiration time.Duration) (strVal string, err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return string(marshalledValue), tx.Set(ctx, key, marshalledValue, expiration).Err()
}

// setObjTxSSZ is a transactional version of setObj
func setObjTxSSZ(ctx context.Context, tx redis.Pipeliner, key string, value *common.GetPayloadResponse, expiration time.Duration) (err error) {
	b, err := value.Capella.Capella.MarshalSSZ()
	if err != nil {
		return err
	}

	return tx.Set(ctx, key, b, expiration).Err()
}

// hSetObjTx is a transactional version of hSetObj
func hSetObjTx(ctx context.Context, tx redis.Pipeliner, key, field string, value any, expiration time.Duration) error {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = tx.HSet(ctx, key, field, marshalledValue).Err()
	if err != nil {
		return err
	}

	return tx.Expire(ctx, key, expiration).Err()
}

// hSetObjTxNoExpire is a transactional version of hSetObj without calling expire
func hSetObjTxNoExpire(ctx context.Context, tx redis.Pipeliner, key, field string, value any, expiration time.Duration) error {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = tx.HSet(ctx, key, field, marshalledValue).Err()
	if err != nil {
		return err
	}

	return nil
}

func (r *redisCache) UpdateActiveValidatorCount(ctx context.Context) error {

	registration, err := r.client.HGetAll(ctx, r.keyValidatorRegistration).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	total := 0

	threeHoursAgo := time.Now().Add(-3 * time.Hour).UnixNano()
	for _, regString := range registration {
		reg := new(ValidatorLatency)
		if err := json.Unmarshal([]byte(regString), reg); err != nil {
			return err
		}

		if reg.LastRegistered > threeHoursAgo {
			total += 1
		}
	}

	return r.client.Set(ctx, r.prefixActiveValidatorsInt, total, 2*time.Hour).Err()
}
