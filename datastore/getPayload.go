package datastore

import (
	"context"
	"errors"
	"strings"

	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var ErrGetPayloadResponseNotFound = errors.New("getPayloadResponse not found")

func (ds *Datastore) GetGetPayloadResponse(ctx context.Context, slot uint64, blockHash string) (*common.GetPayloadResponse, error) {
	blockHash = strings.ToLower(blockHash)

	// 1. try to get from memory
	if !ds.ffDisableBidFullPayloadMemoryCache {
		bidKey := GetPayloadResponseKey{
			Slot:           slot,
			ProposerPubkey: "",
			BlockHash:      blockHash,
		}

		ds.localCache.getPayloadResponsesLock.RLock()
		resp, found := ds.localCache.getPayloadResponses[bidKey]
		ds.localCache.getPayloadResponsesLock.RUnlock()
		if found {
			ds.log.Info("getPayload response from in-memory")
			return resp, nil
		}
	}

	// 2. try to get from Redis
	if !ds.ffDisableBidRedisCache {
		resp, err := ds.redis.getGetPayloadResponse(ctx, slot, blockHash)
		if err != nil {
			return nil, err
		}
		ds.log.Info("getPayload response from redis")
		return resp, nil
	}

	return nil, ErrGetPayloadResponseNotFound
}

func (ds *Datastore) CheckGetPayloadResponse(ctx context.Context, slot uint64, blockHash string) (*common.GetPayloadResponse, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("CheckGetPayloadResponse")
	blockHash = strings.ToLower(blockHash)
	resp, err := ds.redis.getGetPayloadResponse(ctx, slot, blockHash)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	ds.log.Info("getPayload response from redis")
	return resp, nil
}

func (r *redisCache) getGetPayloadResponse(ctx context.Context, slot uint64, blockHash string) (*common.GetPayloadResponse, error) {
	key := r.keyCacheGetPayloadResponse(slot, blockHash)
	resp, err := r.getObjSSZ(ctx, key)
	if errors.Is(err, redis.Nil) {
		return nil, ErrGetPayloadResponseNotFound
	}
	return resp, err
}
