package datastore

import (
	"fmt"
	"time"
)

// keyActiveValidators returns the key for the date + hour of the given time
func (r *redisCache) keyActiveValidators(t time.Time) string {
	return fmt.Sprintf("%s:%s", r.prefixActiveValidators, t.UTC().Format("2006-01-02T15"))
}

// keyActiveValidators returns the key for the date + hour of the given time
func (r *redisCache) keyActiveValidator(pubkey string) string {
	return fmt.Sprintf("%s:%s", r.prefixActiveValidators, pubkey)
}

func (r *redisCache) keyCacheGetHeaderResponse(slot uint64, parentHash, proposerPubkey string) string {
	key := fmt.Sprintf("%s:%d_%s_%s", r.prefixGetHeaderResponse, slot, parentHash, proposerPubkey)
	return key
}

func (r *redisCache) keyCacheGetHeaderResponseList(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s_list", r.prefixGetHeaderResponse, slot, parentHash, proposerPubkey)
}

func (r *redisCache) keyCacheGetPayloadResponse(slot uint64, blockHash string) string {
	return fmt.Sprintf("%s:%d_%s", r.prefixGetPayloadResponse, slot, blockHash)
}

func (r *redisCache) keyBlockSimulationStatus(blockHash string) string {
	return fmt.Sprintf("%s:%s", r.prefixBlockSimulationStatus, blockHash)
}

// keyBlockBuilderLatestBid returns the hashmap key for the getHeader response the latest bid by a specific builder
func (r *redisCache) keyBlockBuilderLatestBids(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBlockBuilderLatestBids, slot, parentHash, proposerPubkey)
}

// keyBlockBuilderLatestBid returns the key for the getHeader response the latest bid by the specified builder
func (r *redisCache) keyBlockBuilderLatestBid(slot uint64, parentHash, proposerPubkey, builderPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s_%s", r.prefixBlockBuilderLatestBids, slot, parentHash, proposerPubkey, builderPubkey)
}

// keyBlockBuilderLatestBidValue returns the hashmap key for the value of the latest bid by a specific builder
func (r *redisCache) keyBlockBuilderLatestBidsValue(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBlockBuilderLatestBidsValue, slot, parentHash, proposerPubkey)
}

// keyBlockBuilderLatestBidsValueTime returns the hashmap key for the value of the latest bid by a specific builder
func (r *redisCache) keyBlockBuilderLatestBidsValueTime(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBlockBuilderLatestBidsValueTime, slot, parentHash, proposerPubkey)
}

// keyBlockBuilderLatestBidValue returns the hashmap key for the time of the latest bid by a specific builder
func (r *redisCache) keyBlockBuilderLatestBidsTime(slot uint64, parentHash, proposerPubkey string) string {
	return fmt.Sprintf("%s:%d_%s_%s", r.prefixBlockBuilderLatestBidsTime, slot, parentHash, proposerPubkey)
}

// keyDeliveredPayload returns the individual delivered payload for a given slot
func (r *redisCache) keyDeliveredPayload(slot uint64) string {
	return fmt.Sprintf("%s:%d", r.prefixDeliveredPayload, slot)
}

// keyBlockBuilderSubmission returns the block builder pubkey for a given block hash
func (r *redisCache) keyBlockBuilderSubmission(blockhash string) string {
	return fmt.Sprintf("%s:%s", r.prefixBlockBuilderSubmission, blockhash)
}

// keyBidTrace returns the key for a given bidtrace
func (r *redisCache) keyBidTrace(blockhash string) string {
	return fmt.Sprintf("%s:%s", r.prefixBidTrace, blockhash)
}
