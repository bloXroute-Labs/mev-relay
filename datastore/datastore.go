// Package datastore helps storing data, utilizing Redis and Postgres as backends
package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/sirupsen/logrus"
)

const databaseRequestTimeout = time.Second * 12

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

// Datastore provides a local memory cache with a Redis and DB backend
type Datastore struct {
	log *logrus.Entry

	redis *RedisCache
	db    database.IDatabaseService

	knownValidatorsByPubkey map[types.PubkeyHex]uint64
	knownValidatorsByIndex  map[uint64]types.PubkeyHex
	knownValidatorsLock     sync.RWMutex

	getHeaderResponsesLock sync.RWMutex
	getHeaderResponses     map[GetHeaderResponseKey]*types.GetHeaderResponse

	GetPayloadResponsesLock sync.RWMutex
	GetPayloadResponses     map[GetPayloadResponseKey]*types.GetPayloadResponse

	// feature flags
	ffDisableBidMemoryCache bool
	ffDisableBidRedisCache  bool
}

func NewDatastore(log *logrus.Entry, redisCache *RedisCache, db database.IDatabaseService) (ds *Datastore, err error) {
	ds = &Datastore{
		log:                     log.WithField("module", "datastore"),
		db:                      db,
		redis:                   redisCache,
		knownValidatorsByPubkey: make(map[types.PubkeyHex]uint64),
		knownValidatorsByIndex:  make(map[uint64]types.PubkeyHex),
		getHeaderResponses:      make(map[GetHeaderResponseKey]*types.GetHeaderResponse),
		GetPayloadResponses:     make(map[GetPayloadResponseKey]*types.GetPayloadResponse),
	}

	if os.Getenv("DISABLE_BID_MEMORY_CACHE") == "1" {
		ds.log.Warn("env: DISABLE_BID_MEMORY_CACHE - disabling in-memory bid cache")
		ds.ffDisableBidMemoryCache = true
	}

	if os.Getenv("DISABLE_BID_REDIS_CACHE") == "1" {
		ds.log.Warn("env: DISABLE_BID_REDIS_CACHE - disabling redis bid cache")
		ds.ffDisableBidRedisCache = true
	}

	return ds, err
}

// RefreshKnownValidators loads known validators from Redis into memory
func (ds *Datastore) RefreshKnownValidators() (cnt int, err error) {
	knownValidators, err := ds.redis.GetKnownValidators()
	if err != nil {
		return 0, err
	}

	knownValidatorsByIndex := make(map[uint64]types.PubkeyHex)
	for pubkey, index := range knownValidators {
		knownValidatorsByIndex[index] = pubkey
	}

	ds.knownValidatorsLock.Lock()
	defer ds.knownValidatorsLock.Unlock()
	ds.knownValidatorsByPubkey = knownValidators
	ds.knownValidatorsByIndex = knownValidatorsByIndex
	return len(knownValidators), nil
}

func (ds *Datastore) IsKnownValidator(pubkeyHex types.PubkeyHex) bool {
	ds.knownValidatorsLock.RLock()
	defer ds.knownValidatorsLock.RUnlock()
	_, found := ds.knownValidatorsByPubkey[pubkeyHex]
	return found
}

func (ds *Datastore) GetKnownValidatorPubkeyByIndex(index uint64) (types.PubkeyHex, bool) {
	ds.knownValidatorsLock.RLock()
	defer ds.knownValidatorsLock.RUnlock()
	pk, found := ds.knownValidatorsByIndex[index]
	return pk, found
}

func (ds *Datastore) NumKnownValidators() int {
	ds.knownValidatorsLock.RLock()
	defer ds.knownValidatorsLock.RUnlock()
	return len(ds.knownValidatorsByIndex)
}

func (ds *Datastore) NumRegisteredValidators() (int64, error) {
	return ds.redis.NumRegisteredValidators()
}

// GetValidatorRegistration returns the validator registration for the given proposerPubkey. If not found then it returns (nil, nil). If
// there's a datastore error, then an error will be returned.
func (ds *Datastore) GetValidatorRegistration(pubkeyHex types.PubkeyHex) (*types.SignedValidatorRegistration, error) {
	return ds.redis.GetValidatorRegistration(pubkeyHex)
}

func (ds *Datastore) GetValidatorRegistrationTimestamp(pubkeyHex types.PubkeyHex) (uint64, error) {
	return ds.redis.GetValidatorRegistrationTimestamp(pubkeyHex)
}

// SetValidatorRegistration saves a validator registration into both Redis and the database
func (ds *Datastore) SetValidatorRegistration(ctx context.Context, entry types.SignedValidatorRegistration) error {
	err := ds.redis.SetValidatorRegistration(entry)
	if err != nil {
		ds.log.WithError(err).WithField("registration", fmt.Sprintf("%+v", entry)).Error("error updating validator registration")
		return err
	}

	err = ds.db.SaveValidatorRegistration(ctx, entry)
	if err != nil {
		ds.log.WithError(err).Error("failed to save validator registration to database")
		return err
	}

	return nil
}

// SaveBlockSubmission stores getHeader and getPayload for later use, to memory and Redis. Note: saving to Postgres not needed, because getHeader doesn't currently check the database, getPayload finds the data it needs through a sql query.
func (ds *Datastore) SaveBlockSubmission(signedBidTrace *types.SignedBidTrace, headerResp *types.GetHeaderResponse, payloadResp *types.GetPayloadResponse) error {
	_blockHash := strings.ToLower(headerResp.Data.Message.Header.BlockHash.String())
	_parentHash := strings.ToLower(headerResp.Data.Message.Header.ParentHash.String())
	_proposerPubkey := strings.ToLower(signedBidTrace.Message.ProposerPubkey.String())

	// Save to memory
	bidKey := GetHeaderResponseKey{
		Slot:           signedBidTrace.Message.Slot,
		ParentHash:     _parentHash,
		ProposerPubkey: _proposerPubkey,
	}

	blockKey := GetPayloadResponseKey{
		Slot:           signedBidTrace.Message.Slot,
		ProposerPubkey: "",
		BlockHash:      _blockHash,
	}

	if err := ds.redis.SaveGetPayloadResponse(signedBidTrace.Message.Slot, _proposerPubkey, payloadResp); err != nil {
		return err
	}

	ds.GetPayloadResponsesLock.Lock()
	ds.GetPayloadResponses[blockKey] = payloadResp
	ds.GetPayloadResponsesLock.Unlock()

	// Save to Redis
	err := ds.redis.SaveGetHeaderResponse(signedBidTrace.Message.Slot, _parentHash, _proposerPubkey, headerResp)
	if err != nil {
		return err
	}
	err = ds.redis.SaveGetHeaderResponseInList(signedBidTrace.Message.Slot, _parentHash, _proposerPubkey, headerResp)
	if err != nil {
		return err
	}

	ds.getHeaderResponsesLock.Lock()
	ds.getHeaderResponses[bidKey] = headerResp
	ds.getHeaderResponsesLock.Unlock()

	ds.log.Infof("saved header and payload to memory, slot: %v, blockNumber: %v, blockHash: %v, parentHash: %v, proposerPublicKey %v, header: %v, payload: %v", signedBidTrace.Message.Slot, payloadResp.Data.BlockNumber, _blockHash, _parentHash, _proposerPubkey, *headerResp, *payloadResp)

	return nil
}

// DeleteBlockSubmission removes a block submission and picks another one for the best bid
func (ds *Datastore) DeleteBlockSubmission(slot uint64, parentHash, proposerPubkey, blockHash string) error {
	bid, err := ds.redis.DeleteBlockSubmission(slot, parentHash, proposerPubkey, blockHash)
	if err != nil {
		return err
	}

	_parentHash := strings.ToLower(bid.Data.Message.Header.ParentHash.String())
	_proposerPubkey := strings.ToLower(proposerPubkey)

	// Save to memory
	bidKey := GetHeaderResponseKey{
		Slot:           slot,
		ParentHash:     _parentHash,
		ProposerPubkey: _proposerPubkey,
	}
	ds.getHeaderResponsesLock.Lock()
	ds.getHeaderResponses[bidKey] = &bid
	ds.getHeaderResponsesLock.Unlock()

	return nil
}

func (ds *Datastore) DeleteBlockSubmissions(slot uint64, parentHash, proposerPubkey string, blockHashes map[string]bool) error {
	bid, err := ds.redis.DeleteBlockSubmissions(slot, parentHash, proposerPubkey, blockHashes)
	if err != nil {
		return err
	}

	_parentHash := strings.ToLower(bid.Data.Message.Header.ParentHash.String())
	_proposerPubkey := strings.ToLower(proposerPubkey)

	// Save to memory
	bidKey := GetHeaderResponseKey{
		Slot:           slot,
		ParentHash:     _parentHash,
		ProposerPubkey: _proposerPubkey,
	}
	ds.getHeaderResponsesLock.Lock()
	ds.getHeaderResponses[bidKey] = &bid
	ds.getHeaderResponsesLock.Unlock()

	return nil
}

func (ds *Datastore) CleanupOldBidsAndBlocks(headSlot uint64) (numRemoved, numRemaining int) {
	ds.getHeaderResponsesLock.Lock()
	for key := range ds.getHeaderResponses {
		if key.Slot < headSlot-1000 {
			delete(ds.getHeaderResponses, key)
			numRemoved++
		}
	}
	numRemaining = len(ds.getHeaderResponses)
	ds.getHeaderResponsesLock.Unlock()

	ds.GetPayloadResponsesLock.Lock()
	for key := range ds.GetPayloadResponses {
		if key.Slot < headSlot-1000 {
			delete(ds.GetPayloadResponses, key)
		}
	}
	ds.GetPayloadResponsesLock.Unlock()
	return
}

// GetGetHeaderResponse returns the bid from memory or Redis
func (ds *Datastore) GetGetHeaderResponse(slot uint64, parentHash, proposerPubkey string) (*types.GetHeaderResponse, error) {
	_parentHash := strings.ToLower(parentHash)
	_proposerPubkey := strings.ToLower(proposerPubkey)

	// 1. Check in memory
	if !ds.ffDisableBidMemoryCache {
		headerKey := GetHeaderResponseKey{
			Slot:           slot,
			ParentHash:     _parentHash,
			ProposerPubkey: _proposerPubkey,
		}

		ds.getHeaderResponsesLock.RLock()
		resp, found := ds.getHeaderResponses[headerKey]
		ds.getHeaderResponsesLock.RUnlock()
		if found {
			ds.log.Info("getHeader response from in-memory")
			return resp, nil
		}
	}

	// 2. Check in Redis
	resp, err := ds.redis.GetGetHeaderResponse(slot, _parentHash, _proposerPubkey)
	if err != nil {
		return nil, err
	}

	ds.log.Info("getHeader response from redis")
	return resp, nil
}

// GetGetPayloadResponse returns the getPayload response from memory or Redis or Database
func (ds *Datastore) GetGetPayloadResponse(ctx context.Context, slot uint64, blockHash string) (*types.GetPayloadResponse, error) {
	// _proposerPubkey := strings.ToLower(proposerPubkey)
	_blockHash := strings.ToLower(blockHash)

	// 1. try to get from memory
	if !ds.ffDisableBidMemoryCache {
		bidKey := GetPayloadResponseKey{
			Slot:           slot,
			ProposerPubkey: "",
			BlockHash:      _blockHash,
		}

		ds.GetPayloadResponsesLock.RLock()
		resp, found := ds.GetPayloadResponses[bidKey]
		ds.GetPayloadResponsesLock.RUnlock()
		if found {
			ds.log.Info("getPayload response from in-memory")
			return resp, nil
		}
	}

	// 2. try to get from Redis
	if !ds.ffDisableBidRedisCache {
		resp, err := ds.redis.GetGetPayloadResponse(slot, _blockHash)
		if err == nil {
			ds.log.Info("getPayload response from redis")
			return resp, nil
		}
	}

	// 3. try to get from database
	blockSubEntry, err := ds.db.GetExecutionPayloadEntryBySlotPkHash(ctx, slot, blockHash)
	if err != nil {
		return nil, err
	}

	// deserialize execution payload
	executionPayload := new(types.ExecutionPayload)
	err = json.Unmarshal([]byte(blockSubEntry.Payload), executionPayload)
	if err != nil {
		return nil, err
	}

	ds.log.Info("getPayload response from database")
	return &types.GetPayloadResponse{
		Version: types.VersionString(blockSubEntry.Version),
		Data:    executionPayload,
	}, nil
}

func (ds *Datastore) SetValidatorRegistrationMap(data map[string]interface{}) error {
	if err := ds.redis.SetValidatorRegistrationMap(data); err != nil {
		return err
	}
	return nil
}

func (ds *Datastore) SaveValidatorRegistration(registration types.SignedValidatorRegistration) error {
	ctx, cancel := context.WithTimeout(context.Background(), databaseRequestTimeout)
	defer cancel()
	return ds.db.SaveValidatorRegistration(ctx, registration)
}
