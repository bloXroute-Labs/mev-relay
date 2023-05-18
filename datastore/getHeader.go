package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type dataSource string

const (
	LocalMemory dataSource = "local memory"
	RedisCache  dataSource = "redis"
)

var ErrGetHeaderResponseNotFound = errors.New("getHeader response not found")

// GetGetHeaderResponse returns the bid from memory or Redis
func (ds *Datastore) GetGetHeaderResponse(ctx context.Context, slot uint64, parentHash, proposerPubkey string) ([]byte, *common.GetHeaderResponse, dataSource, error) {
	start := time.Now()
	span := trace.SpanFromContext(ctx)
	span.AddEvent("GetGetHeaderResponse")
	parentHash = strings.ToLower(parentHash)
	proposerPubkey = strings.ToLower(proposerPubkey)

	// check in memory
	resp := ds.getGetHeaderResponseLocal(slot, parentHash, proposerPubkey)
	if resp != nil {
		ds.log.WithFields(logrus.Fields{
			"duration":  time.Since(start).Milliseconds(),
			"blockHash": resp.Capella.Capella.Message.Header.BlockHash.String(),
			"value":     resp.Capella.Capella.Message.Value.String(),
			"slot":      slot,
		}).Info("getHeader response from in-memory")
		span.SetAttributes(
			attribute.String("data-source", "memory"),
		)
		raw, _ := json.Marshal(resp)
		return raw, resp, LocalMemory, nil
	}

	// check in Redis
	raw, err := ds.redis.getGetHeaderResponseString(ctx, slot, parentHash, proposerPubkey)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, "", err
	}

	err = json.Unmarshal([]byte(raw), &resp)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, nil, "", fmt.Errorf("failed to unmarshal getHeader response: %w", err)
	}

	ds.log.WithFields(logrus.Fields{
		"duration":  time.Since(start).Milliseconds(),
		"blockHash": resp.Capella.Capella.Message.Header.BlockHash.String(),
		"value":     resp.Capella.Capella.Message.Value.String(),
		"slot":      slot,
	}).Info("getHeader response from redis")
	span.SetAttributes(
		attribute.String("data-source", "redis"),
	)
	return []byte(raw), resp, RedisCache, nil
}

func (r *redisCache) getGetHeaderResponseString(ctx context.Context, slot uint64, parentHash, proposerPubkey string) (string, error) {
	key := r.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrGetHeaderResponseNotFound
	}
	return val, err
}

func (ds *Datastore) getGetHeaderResponseLocal(slot uint64, parentHash, proposerPubkey string) *common.GetHeaderResponse {
	if ds.ffDisableBidMemoryCache {
		return nil
	}

	headerKey := GetHeaderResponseKey{
		Slot:           slot,
		ParentHash:     parentHash,
		ProposerPubkey: proposerPubkey,
	}

	ds.localCache.getHeaderResponsesLock.RLock()
	defer ds.localCache.getHeaderResponsesLock.RUnlock()

	return ds.localCache.getHeaderResponses[headerKey]
}

func (ds *Datastore) getGetHeaderResponseTx(ctx context.Context, tx redis.Pipeliner, slot uint64, parentHash, proposerPubkey string) (*common.GetHeaderResponse, error) {
	start := time.Now()
	parentHash = strings.ToLower(parentHash)
	proposerPubkey = strings.ToLower(proposerPubkey)

	// check in memory
	resp := ds.getGetHeaderResponseLocal(slot, parentHash, proposerPubkey)
	if resp != nil {
		ds.log.WithField("duration", time.Since(start).Milliseconds()).Info("getHeader response from in-memory")
		return resp, nil
	}

	// check in Redis
	key := ds.redis.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	c := tx.Get(ctx, key)
	_, err := tx.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	value, err := c.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if value == "" {
		return nil, ErrGetHeaderResponseNotFound
	}

	err = json.Unmarshal([]byte(value), &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal getHeader response: %w", err)
	}

	return resp, err
}
