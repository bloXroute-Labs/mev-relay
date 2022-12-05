package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flashbots/go-boost-utils/types"
	"github.com/go-redis/redis/v9"
)

var (
	redisPrefix = "boost-relay"

	expiryBidCache = 1 * time.Minute

	RedisConfigFieldPubkey    = "pubkey"
	RedisStatsFieldLatestSlot = "latest-slot"
)

func PubkeyHexToLowerStr(pk types.PubkeyHex) string {
	return strings.ToLower(string(pk))
}

func connectRedis(redisURI string, connectionPoolLimit int) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		PoolSize: connectionPoolLimit,
		Addr:     redisURI,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		// unable to connect to redis
		return nil, err
	}
	return redisClient, nil
}

type RedisCache struct {
	client *redis.Client

	prefixGetHeaderResponse  string
	prefixGetPayloadResponse string

	keyKnownValidators                string
	keyValidatorRegistration          string
	keyValidatorRegistrationTimestamp string

	keyRelayConfig    string
	keyStats          string
	keyProposerDuties string

	getHeaderList sync.Mutex
}

func NewRedisCache(redisURI, prefix string, connectionPoolLimit int) (*RedisCache, error) {
	client, err := connectRedis(redisURI, connectionPoolLimit)
	if err != nil {
		return nil, err
	}

	return &RedisCache{
		client: client,

		prefixGetHeaderResponse:  fmt.Sprintf("%s/%s:cache-gethead-response", redisPrefix, prefix),
		prefixGetPayloadResponse: fmt.Sprintf("%s/%s:cache-getpayload-response", redisPrefix, prefix),

		keyKnownValidators:                fmt.Sprintf("%s/%s:known-validators", redisPrefix, prefix),
		keyValidatorRegistration:          fmt.Sprintf("%s/%s:validators-registration-timestamp", redisPrefix, prefix),
		keyValidatorRegistrationTimestamp: fmt.Sprintf("%s/%s:validators-registration", redisPrefix, prefix),
		keyRelayConfig:                    fmt.Sprintf("%s/%s:relay-config", redisPrefix, prefix),

		keyStats:          fmt.Sprintf("%s/%s:stats", redisPrefix, prefix),
		keyProposerDuties: fmt.Sprintf("%s/%s:proposer-duties", redisPrefix, prefix),
	}, nil
}

func (r *RedisCache) keyCacheGetHeaderResponse(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixGetHeaderResponse, slot, parentHash, proposerPubkey)
}

func (r *RedisCache) keyCacheGetHeaderResponseList(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s_list", r.prefixGetHeaderResponse, slot, parentHash, proposerPubkey)
}

func (r *RedisCache) keyCacheGetPayloadResponse(slot uint64, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s", r.prefixGetPayloadResponse, slot, blockHash)
}

func (r *RedisCache) GetObj(key string, obj any) (err error) {
	value, err := r.client.Get(context.Background(), key).Result()
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(value), &obj)
}

func (r *RedisCache) SetObj(key string, value any, expiration time.Duration) (err error) {
	marshalledValue, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return r.client.Set(context.Background(), key, marshalledValue, expiration).Err()
}

func (r *RedisCache) GetKnownValidators() (map[types.PubkeyHex]uint64, error) {
	validators := make(map[types.PubkeyHex]uint64)
	entries, err := r.client.HGetAll(context.Background(), r.keyKnownValidators).Result()
	if err != nil {
		return nil, err
	}
	for pubkey, proposerIndexStr := range entries {
		proposerIndex, err := strconv.ParseUint(proposerIndexStr, 10, 64)
		if err == nil {
			validators[types.PubkeyHex(pubkey)] = proposerIndex
		}
	}
	return validators, nil
}

func (r *RedisCache) SetKnownValidator(pubkeyHex types.PubkeyHex, proposerIndex uint64) error {
	return r.client.HSet(context.Background(), r.keyKnownValidators, PubkeyHexToLowerStr(pubkeyHex), proposerIndex).Err()
}

func (r *RedisCache) GetValidatorRegistration(proposerPubkey types.PubkeyHex) (*types.SignedValidatorRegistration, error) {
	registration := new(types.SignedValidatorRegistration)
	value, err := r.client.HGet(context.Background(), r.keyValidatorRegistration, strings.ToLower(proposerPubkey.String())).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(value), registration)
	return registration, err
}

func (r *RedisCache) GetValidatorRegistrationTimestamp(proposerPubkey types.PubkeyHex) (uint64, error) {
	timestamp, err := r.client.HGet(context.Background(), r.keyValidatorRegistrationTimestamp, strings.ToLower(proposerPubkey.String())).Uint64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return timestamp, err
}

func (r *RedisCache) SetValidatorRegistration(entry types.SignedValidatorRegistration) error {
	err := r.client.HSet(context.Background(), r.keyValidatorRegistrationTimestamp, strings.ToLower(entry.Message.Pubkey.PubkeyHex().String()), entry.Message.Timestamp).Err()
	if err != nil {
		return err
	}

	marshalledValue, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	err = r.client.HSet(context.Background(), r.keyValidatorRegistration, strings.ToLower(entry.Message.Pubkey.PubkeyHex().String()), marshalledValue).Err()
	return err
}

type AliasSignedValidatorRegistration types.SignedValidatorRegistration

func (i AliasSignedValidatorRegistration) MarshalBinary() ([]byte, error) {
	return json.Marshal(i)
}

func (r *RedisCache) SetValidatorRegistrationMap(data map[string]interface{}) error {
	return r.client.HSet(context.Background(), r.keyValidatorRegistration, data).Err()
}

func (r *RedisCache) SetValidatorRegistrations(entries []types.SignedValidatorRegistration) error {
	for _, entry := range entries {
		err := r.SetValidatorRegistration(entry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisCache) NumRegisteredValidators() (int64, error) {
	return r.client.HLen(context.Background(), r.keyValidatorRegistrationTimestamp).Result()
}

func (r *RedisCache) SetStats(field string, value any) (err error) {
	return r.client.HSet(context.Background(), r.keyStats, field, value).Err()
}

func (r *RedisCache) GetStats(field string) (value string, err error) {
	return r.client.HGet(context.Background(), r.keyStats, field).Result()
}

func (r *RedisCache) SetProposerDuties(proposerDuties []types.BuilderGetValidatorsResponseEntry) (err error) {
	return r.SetObj(r.keyProposerDuties, proposerDuties, 0)
}

func (r *RedisCache) GetProposerDuties() (proposerDuties []types.BuilderGetValidatorsResponseEntry, err error) {
	proposerDuties = make([]types.BuilderGetValidatorsResponseEntry, 0)
	err = r.GetObj(r.keyProposerDuties, &proposerDuties)
	if errors.Is(err, redis.Nil) {
		return proposerDuties, nil
	}
	return proposerDuties, err
}

func (r *RedisCache) SetRelayConfig(field, value string) (err error) {
	return r.client.HSet(context.Background(), r.keyRelayConfig, field, value).Err()
}

func (r *RedisCache) GetRelayConfig(field string) (string, error) {
	res, err := r.client.HGet(context.Background(), r.keyRelayConfig, field).Result()
	if errors.Is(err, redis.Nil) {
		return res, nil
	}
	return res, err
}

func (r *RedisCache) SaveGetHeaderResponse(slot uint64, parentHash, proposerPubkey string, headerResp *types.GetHeaderResponse) (err error) {
	key := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	return r.SetObj(key, headerResp, expiryBidCache)
}

func (r *RedisCache) SaveGetHeaderResponseInList(slot uint64, parentHash, proposerPubkey string, headerResp *types.GetHeaderResponse) error {
	key := r.keyCacheGetHeaderResponseList(slot, parentHash, proposerPubkey)
	bids := []types.GetHeaderResponse{}
	r.getHeaderList.Lock()
	err := r.GetObj(key, &bids)
	r.getHeaderList.Unlock()
	if err != nil && err != redis.Nil {
		return err
	}
	bids = append(bids, *headerResp)
	r.getHeaderList.Lock()
	err = r.SetObj(key, bids, expiryBidCache)
	r.getHeaderList.Unlock()
	return err
}

func (r *RedisCache) DeleteBlockSubmission(slot uint64, parentHash, proposerPubkey, blockHash string) (types.GetHeaderResponse, error) {
	maxBid := types.GetHeaderResponse{}

	key := r.keyCacheGetHeaderResponseList(slot, parentHash, proposerPubkey)
	bids := []types.GetHeaderResponse{}
	r.getHeaderList.Lock()
	err := r.GetObj(key, &bids)
	r.getHeaderList.Unlock()
	if err != nil {
		return maxBid, err
	}

	maxBidValue := big.NewInt(0)
	newBidList := []types.GetHeaderResponse{}
	for _, bid := range bids {
		if strings.EqualFold(bid.Data.Message.Header.BlockHash.String(), blockHash) {
			continue
		}
		newBidList = append(newBidList, bid)
		if maxBidValue.Cmp(bid.Data.Message.Value.BigInt()) < 0 {
			maxBidValue = bid.Data.Message.Value.BigInt()
			maxBid = bid
		}
	}

	if maxBid.Data == nil {
		return maxBid, errors.New("max SignedBuilderBid is nil")
	}

	if err := r.SaveGetHeaderResponse(slot, parentHash, proposerPubkey, &maxBid); err != nil {
		return maxBid, err
	}

	r.getHeaderList.Lock()
	err = r.SetObj(key, newBidList, expiryBidCache)
	r.getHeaderList.Unlock()
	return maxBid, err
}

func (r *RedisCache) DeleteBlockSubmissions(slot uint64, parentHash, proposerPubkey string, blockHash map[string]bool) (types.GetHeaderResponse, error) {
	maxBid := types.GetHeaderResponse{}

	key := r.keyCacheGetHeaderResponseList(slot, parentHash, proposerPubkey)
	bids := []types.GetHeaderResponse{}
	r.getHeaderList.Lock()
	err := r.GetObj(key, &bids)
	r.getHeaderList.Unlock()
	if err != nil {
		return maxBid, err
	}

	newBidList := []types.GetHeaderResponse{}
	maxBidValue := big.NewInt(0)
	for _, bid := range bids {
		if _, ok := blockHash[strings.ToLower(bid.Data.Message.Header.BlockHash.String())]; ok {
			continue
		}
		newBidList = append(newBidList, bid)
		if maxBidValue.Cmp(bid.Data.Message.Value.BigInt()) < 0 {
			maxBidValue = bid.Data.Message.Value.BigInt()
			maxBid = bid
		}
	}

	if maxBid.Data == nil {
		return maxBid, errors.New("max SignedBuilderBid is nil")
	}

	if err := r.SaveGetHeaderResponse(slot, parentHash, proposerPubkey, &maxBid); err != nil {
		return maxBid, err
	}

	r.getHeaderList.Lock()
	err = r.SetObj(key, newBidList, expiryBidCache)
	r.getHeaderList.Unlock()
	return maxBid, err
}

func (r *RedisCache) GetGetHeaderResponse(slot uint64, parentHash, proposerPubkey string) (*types.GetHeaderResponse, error) {
	key := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	resp := new(types.GetHeaderResponse)
	err := r.GetObj(key, resp)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return resp, err
}

func (r *RedisCache) SaveGetPayloadResponse(slot uint64, proposerPubkey string, resp *types.GetPayloadResponse) (err error) {
	key := r.keyCacheGetPayloadResponse(slot, resp.Data.BlockHash.String())
	return r.SetObj(key, resp, expiryBidCache)
}

func (r *RedisCache) GetGetPayloadResponse(slot uint64, blockHash string) (*types.GetPayloadResponse, error) {
	key := r.keyCacheGetPayloadResponse(slot, blockHash)
	resp := new(types.GetPayloadResponse)
	err := r.GetObj(key, resp)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return resp, err
}
