package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/redis/go-redis/v9"
)

func (ds *Datastore) DeleteBlockSubmissions(ctx context.Context, slot uint64, parentHash, proposerPubkey string, blockHashes map[string]struct{}) error {
	bid, err := ds.redis.deleteBlockSubmissions(ctx, slot, parentHash, proposerPubkey, blockHashes)
	if err != nil {
		return err
	}

	_parentHash := strings.ToLower(bid.Capella.Capella.Message.Header.ParentHash.String())
	_proposerPubkey := strings.ToLower(proposerPubkey)

	// Save to memory
	bidKey := GetHeaderResponseKey{
		Slot:           slot,
		ParentHash:     _parentHash,
		ProposerPubkey: _proposerPubkey,
	}
	ds.localCache.getHeaderResponsesLock.Lock()
	ds.localCache.getHeaderResponses[bidKey] = &bid
	ds.localCache.getHeaderResponsesLock.Unlock()

	return nil
}

func (ds *Datastore) CleanupOldBidsAndBlocks(headSlot uint64) (numRemoved, numRemaining int) {
	ds.localCache.getHeaderResponsesLock.Lock()
	for key := range ds.localCache.getHeaderResponses {
		if key.Slot < headSlot-1000 {
			delete(ds.localCache.getHeaderResponses, key)
			numRemoved++
		}
	}
	numRemaining = len(ds.localCache.getHeaderResponses)
	ds.localCache.getHeaderResponsesLock.Unlock()

	ds.localCache.getPayloadResponsesLock.Lock()
	for key := range ds.localCache.getHeaderResponses {
		if key.Slot < headSlot-1000 {
			delete(ds.localCache.getHeaderResponses, key)
		}
	}
	ds.localCache.getPayloadResponsesLock.Unlock()
	return
}

func (ds *Datastore) GetBlockSubmissionStatus(ctx context.Context, blockHash string) (string, error) {
	key := ds.redis.keyBlockSimulationStatus(blockHash)
	status, err := ds.redis.getString(ctx, key)
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return status, err
}

func (ds *Datastore) GetSetBlockSubmissionStatus(ctx context.Context, blockHash string, status BlockSimulationStatus) (string, error) {
	key := ds.redis.keyBlockSimulationStatus(blockHash)
	marshalledValue, err := json.Marshal(status)
	if err != nil {
		return "", err
	}
	setArgs := redis.SetArgs{
		Mode:     "NX",
		Get:      true,
		ExpireAt: time.Now().Add(30 * time.Second),
	}
	return ds.redis.client.SetArgs(ctx, key, marshalledValue, setArgs).Result()
}

func (ds *Datastore) SetBlockSubmissionStatus(ctx context.Context, blockHash string, status BlockSimulationStatus) error {
	key := ds.redis.keyBlockSimulationStatus(blockHash)
	return ds.redis.setObj(ctx, key, status, expiryBlockSubmissionStatus)
}

func (r *redisCache) deleteBlockSubmissions(ctx context.Context, slot uint64, parentHash, proposerPubkey string, blockHash map[string]struct{}) (common.GetHeaderResponse, error) {
	maxBid := common.GetHeaderResponse{}

	key := r.keyCacheGetHeaderResponseList(slot, parentHash, proposerPubkey)

	var bids []common.GetHeaderResponse
	r.getHeaderList.Lock()
	err := r.getObj(ctx, key, &bids)
	r.getHeaderList.Unlock()
	if err != nil {
		return maxBid, err
	}

	var newBidList []common.GetHeaderResponse
	maxBidValue := big.NewInt(0)
	for _, bid := range bids {
		if _, ok := blockHash[strings.ToLower(bid.Capella.Capella.Message.Header.BlockHash.String())]; ok {
			continue
		}
		newBidList = append(newBidList, bid)
		if maxBidValue.Cmp(bid.Capella.Capella.Message.Value.ToBig()) < 0 {
			maxBidValue = bid.Capella.Capella.Message.Value.ToBig()
			maxBid = bid
		}
	}

	if maxBid.Capella.Capella == nil {
		return maxBid, errors.New("max SignedBuilderBid is nil")
	}

	if err := r.saveGetHeaderResponse(ctx, slot, parentHash, proposerPubkey, &maxBid); err != nil {
		return maxBid, err
	}

	r.getHeaderList.Lock()
	err = r.setObj(ctx, key, newBidList, expiryBidCache)
	r.getHeaderList.Unlock()
	return maxBid, err
}

func (r *redisCache) saveGetHeaderResponse(ctx context.Context, slot uint64, parentHash, proposerPubkey string, headerResp *common.GetHeaderResponse) (err error) {
	key := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	return r.setObj(ctx, key, headerResp, expiryBidCache)
}
