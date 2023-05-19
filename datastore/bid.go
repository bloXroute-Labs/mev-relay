package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/attestantio/go-builder-client/api"
	v1 "github.com/attestantio/go-builder-client/api/v1"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/redis/go-redis/v9"
	"go.opencensus.io/trace"
)

var ErrFailedUpdatingTopBidNoBids = errors.New("failed to update top bid because no bids were found")

type saveBlockStats struct {
	GetSlotBestHeaderStartTime       string `json:"get_slot_best_header_start_time"`
	GetSlotBestHeaderDurationMS      int64  `json:"get_slot_best_header_duration_ms"`
	SaveGetPayloadResponseStartTime  string `json:"save_get_payload_response_start_time"`
	SaveGetPayloadResponseDurationMS int64  `json:"save_get_payload_response_duration_ms"`
	SaveBuilderLatestBidStartTime    string `json:"save_builder_latest_bid_start_time"`
	SaveBuilderLatestBidDurationMS   int64  `json:"save_builder_latest_bid_duration_ms"`
	PipelineExec1StartTime           string `json:"pipeline_exec_1_start_time"`
	PipelineExec1DurationMS          int64  `json:"pipeline_exec_1_duration_ms"`
	UpdateTopBidStartTime            string `json:"update_top_bid_start_time"`
	UpdateTopBidDurationMS           int64  `json:"update_top_bid_duration_ms"`
	PipelineExec2StartTime           string `json:"pipeline_exec_2_start_time"`
	PipelineExec2DurationMS          int64  `json:"pipeline_exec_2_duration_ms"`
	SaveBlockSubmissionStartTime     string `json:"save_block_submission_start_time"`
	SaveBlockSubmissionDurationMS    int64  `json:"save_block_submission_duration_ms"`
}

func (ds *Datastore) SaveBlock(
	ctx context.Context,
	getHeaderResponse *common.GetHeaderResponse,
	payload *capella.ExecutionPayload,
	bidTrace *v1.BidTrace,
	receivedAt time.Time,
	slotExpirationChan chan *syncmap.SyncMap[uint64, []string],
	tier sdnmessage.AccountTier,
) (isMostProfitable bool, saveStats *saveBlockStats, err error) {

	_, span := trace.StartSpan(ctx, "SaveBlock")
	defer span.End()

	slot := bidTrace.Slot
	proposerPubkey := strings.ToLower(bidTrace.ProposerPubkey.String())
	parentHash := strings.ToLower(bidTrace.ParentHash.String())
	builderPubKey := bidTrace.BuilderPubkey.String()
	blockHash := bidTrace.BlockHash.String()
	value := bidTrace.Value.ToBig()

	ds.log.WithField("bidtrace", bidTrace).Info("received bidtrace")

	data := &api.VersionedExecutionPayload{
		Version: consensusspec.DataVersionCapella,
		Capella: payload,
	}

	getPayloadResponse := &common.GetPayloadResponse{
		Capella: data,
	}

	saveStats = &saveBlockStats{}

	_, err = ds.redis.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// TODO: may need to check if context has been cancelled in between each step depending on pipeline cancel behavior
		blockHashLower := strings.ToLower(blockHash)

		defer func() {
			select {
			case <-ctx.Done():
				ds.log.Infof("pipeline canceled to save block %s from builder %s for slot %v valued at %v", blockHash, bidTrace.BuilderPubkey, slot, common.WeiToEth(bidTrace.Value.String()))
			default:
				ds.log.Infof("pipeline completed to save block %s from builder %s for slot %v", blockHash, bidTrace.BuilderPubkey, slot)
			}
		}()

		currentLatestBuilderBidTimeValue, err := ds.getBlockBuilderLatestBidsTimeValueTx(ctx, pipe, slot, parentHash, proposerPubkey, builderPubKey)
		if err != nil {
			return fmt.Errorf("failed to get current latest builder bid timestamp, error: %w", err)
		}

		currentLatestBuilderBidTimeUnix, err := timeValueToUnixms(currentLatestBuilderBidTimeValue)
		if err != nil {
			return fmt.Errorf("failed to get current latest builder bid timestamp, error: %w", err)
		}

		currentLatestBuilderBidTime := time.UnixMilli(currentLatestBuilderBidTimeUnix)
		ds.log.Infof("current latest builder bid time unix: %v, block %s received at: %v", currentLatestBuilderBidTimeUnix, blockHash, receivedAt.UnixMilli())
		if currentLatestBuilderBidTime.After(receivedAt) {
			return fmt.Errorf("current latest builder bid timestamp %s is later than new block %s timestamp %s", currentLatestBuilderBidTime, blockHash, receivedAt)
		}

		latestValue, err := timeValueToValue(currentLatestBuilderBidTimeValue)
		if err != nil {
			return fmt.Errorf("failed to get current latest builder bid value error: %w", err)
		}

		if !tier.IsEnterprise() && bidTrace.Value.ToBig().Cmp(latestValue) < 1 {
			return fmt.Errorf("not saving lower bid for low tier account error %w", err)
		}

		getSlotBestHeaderStartTime := time.Now().UTC()
		slotBestHeader, err := ds.getGetHeaderResponseTx(ctx, pipe, slot, payload.ParentHash.String(), proposerPubkey)
		if err != nil && !errors.Is(err, ErrGetHeaderResponseNotFound) {
			return fmt.Errorf("failed to get slot best header, error: %w", err)
		}

		if slotBestHeader != nil {
			isMostProfitable = slotBestHeader.Capella.Capella.Message.Value.ToBig().Cmp(getHeaderResponse.Capella.Capella.Message.Value.ToBig()) < 0
		}
		saveStats.GetSlotBestHeaderDurationMS = time.Since(getSlotBestHeaderStartTime).Milliseconds()
		saveStats.GetSlotBestHeaderStartTime = getSlotBestHeaderStartTime.String()

		saveGetPayloadResponseStartTime := time.Now().UTC()
		err = ds.SaveGetPayloadResponseTx(ctx, pipe, slot, getPayloadResponse)
		if err != nil {
			return fmt.Errorf("failed to save get payload response: %w", err)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		saveStats.SaveGetPayloadResponseDurationMS = time.Since(saveGetPayloadResponseStartTime).Milliseconds()
		saveStats.SaveGetPayloadResponseStartTime = saveGetPayloadResponseStartTime.String()

		// save this builder's latest bid
		saveBuilderLatestBidStartTime := time.Now().UTC()
		keysToExpire, bidStr, err := ds.SaveLatestBuilderBidTxSingleMap(ctx, pipe, slot, builderPubKey,
			parentHash, proposerPubkey, receivedAt, getHeaderResponse)
		if err != nil {
			return fmt.Errorf("failed to save latest builder bid: %w", err)
		}
		saveStats.SaveBuilderLatestBidDurationMS = time.Since(saveBuilderLatestBidStartTime).Milliseconds()
		saveStats.SaveBuilderLatestBidStartTime = saveBuilderLatestBidStartTime.String()
		go func(slot uint64, keysToExpire []string) {
			m := syncmap.NewIntegerMapOf[uint64, []string]()
			m.Store(slot, keysToExpire)
			slotExpirationChan <- m
		}(slot, keysToExpire)

		pipelineExec1StartTime := time.Now().UTC()
		_, err = pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("failed to execute pipeline: %w", err)
		}
		saveStats.PipelineExec1DurationMS = time.Since(pipelineExec1StartTime).Milliseconds()
		saveStats.PipelineExec1StartTime = pipelineExec1StartTime.String()

		// update the top bid
		updateTopBidStartTime := time.Now().UTC()
		topBidUpdated, err := ds.UpdateTopBidTx(ctx, pipe, slot, parentHash, proposerPubkey, builderPubKey, value, bidStr)
		if err != nil {
			return fmt.Errorf("failed to update top bid: %w", err)
		}

		// if the top bid wasn't updated, we don't need to save the block submission
		if !topBidUpdated {
			return err
		}
		saveStats.UpdateTopBidDurationMS = time.Since(updateTopBidStartTime).Milliseconds()
		saveStats.UpdateTopBidStartTime = updateTopBidStartTime.String()

		pipelineExec2StartTime := time.Now().UTC()
		_, err = pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		saveStats.PipelineExec2DurationMS = time.Since(pipelineExec2StartTime).Milliseconds()
		saveStats.PipelineExec2StartTime = pipelineExec2StartTime.String()

		// save the block submission
		saveBlockSubmissionStartTime := time.Now().UTC()
		err = ds.SaveBlockSubmissionTx(ctx, pipe, bidTrace, getHeaderResponse, data, blockHashLower, parentHash, proposerPubkey)
		if err != nil {
			return err
		}
		saveStats.SaveBlockSubmissionDurationMS = time.Since(saveBlockSubmissionStartTime).Milliseconds()
		saveStats.SaveBlockSubmissionStartTime = saveBlockSubmissionStartTime.String()
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		err = fmt.Errorf("failed to execute pipeline: %w", err)
		return
	}

	// discard redis.Nil error
	err = nil

	return
}

func timeValueToUnixms(value string) (int64, error) {
	splitValues := strings.Split(value, ",")
	if len(splitValues) != 2 {
		return 0, nil
	}

	unixMilli, err := strconv.ParseInt(splitValues[1], 10, 64)
	if err != nil {
		return 0, err
	}

	return unixMilli, nil
}

func timeValueToValue(value string) (*big.Int, error) {
	splitValues := strings.Split(value, ",")
	if len(splitValues) != 2 {
		return nil, nil
	}

	b, ok := big.NewInt(0).SetString(splitValues[0], 10)
	if !ok {
		return nil, errors.New("could not get value from string")
	}
	return b, nil
}

// SaveGetPayloadResponseTx saves the payload response
func (ds *Datastore) SaveGetPayloadResponseTx(ctx context.Context, tx redis.Pipeliner, slot uint64, resp *common.GetPayloadResponse) error {
	key := ds.redis.keyCacheGetPayloadResponse(slot, resp.Capella.Capella.BlockHash.String())
	return setObjTxSSZ(ctx, tx, key, resp, expiryBidCache)
}

// SaveLatestBuilderBidTx saves the latest bid by a specific builder
func (ds *Datastore) SaveLatestBuilderBidTx(ctx context.Context, tx redis.Pipeliner, slot uint64, builderPubkey, parentHash, proposerPubkey string, receivedAt time.Time, headerResp *common.GetHeaderResponse) (keysToExpire []string, err error) {
	keyLatestBids := ds.redis.keyBlockBuilderLatestBids(slot, parentHash, proposerPubkey)
	err = hSetObjTxNoExpire(ctx, tx, keyLatestBids, builderPubkey, headerResp, expiryBidCache)
	if err != nil {
		return nil, err
	}

	// set the time of the request
	keyLatestBidsTime := ds.redis.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	err = tx.HSet(ctx, keyLatestBidsTime, builderPubkey, receivedAt.UnixMilli()).Err()
	if err != nil {
		return nil, err
	}

	// set the value last, because that's iterated over when updating the best bid, and the payload has to be available
	keyLatestBidsValue := ds.redis.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	err = tx.HSet(ctx, keyLatestBidsValue, builderPubkey, headerResp.Capella.Capella.Message.Value.ToBig().String()).Err()
	if err != nil {
		return nil, err
	}

	return []string{keyLatestBids, keyLatestBidsTime, keyLatestBidsValue}, nil
}

// SaveLatestBuilderBidTxSingleMap saves the latest bid by a specific builder using single map
func (ds *Datastore) SaveLatestBuilderBidTxSingleMap(ctx context.Context, tx redis.Pipeliner, slot uint64, builderPubkey, parentHash, proposerPubkey string, receivedAt time.Time, headerResp *common.GetHeaderResponse) (keysToExpire []string, bidStr string, err error) {
	start := time.Now()

	keyLatestBids := ds.redis.keyBlockBuilderLatestBid(slot, parentHash, proposerPubkey, builderPubkey)
	bidStr, err = setObjTxWithReturn(ctx, tx, keyLatestBids, headerResp, expiryBidCache)
	if err != nil {
		return nil, bidStr, err
	}

	if _, err := tx.Exec(ctx); err != nil {
		return nil, bidStr, err
	}
	ds.log.Infof("redisDebugTiming: setBuilderLatestBidTimeMS: %v", time.Since(start).Milliseconds())

	valueTime := fmt.Sprintf("%v-%v", headerResp.Capella.Capella.Message.Value.ToBig().String(), receivedAt.UnixMilli())

	start2 := time.Now()
	// set the value last, because that's iterated over when updating the best bid, and the payload has to be available
	keyLatestBidsValue := ds.redis.keyBlockBuilderLatestBidsValueTime(slot, parentHash, proposerPubkey)
	err = tx.HSet(ctx, keyLatestBidsValue, builderPubkey, valueTime).Err()
	if err != nil {
		return nil, bidStr, err
	}
	if _, err := tx.Exec(ctx); err != nil {
		return nil, bidStr, err
	}
	ds.log.Infof("redisDebugTiming: hsetBuilderLatestBidValueTime: %v", time.Since(start2).Milliseconds())
	return []string{keyLatestBidsValue}, bidStr, nil
}

// UpdateTopBidTx updates the top bid for a specific slot if needed
func (ds *Datastore) UpdateTopBidTx(ctx context.Context, tx redis.Pipeliner, slot uint64, parentHash, proposerPubkey, builderPubkey string, newBidValue *big.Int, newBidStr string) (updated bool, err error) {

	updateImmediately := false
	newTopBidStr := ""

	keyTopBid := ds.redis.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
	currentTopBidCmd := tx.Get(ctx, keyTopBid)
	_, err = tx.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	currentTopBid, err := currentTopBidCmd.Result()
	if err != nil && err != redis.Nil {
		return false, ErrFailedUpdatingTopBidNoBids
	}

	if newBidStr == currentTopBid {
		return false, nil
	}

	if currentTopBid == "" || err == redis.Nil {
		updateImmediately = true
		newTopBidStr = newBidStr
	}

	if !updateImmediately {
		topBid := new(common.GetHeaderResponse)
		if err := json.Unmarshal([]byte(currentTopBid), topBid); err != nil {
			return false, err
		}

		builder, err := topBid.Capella.Builder()
		if err != nil {
			return false, err
		}

		// if the new bid is higher than the top, update immediately
		if newBidValue.Cmp(topBid.Value()) > 0 {
			updateImmediately = true
			newTopBidStr = newBidStr
		}

		// if new bid builder is not the top bid and it is a lower bid we can return early
		if builder.String() != builderPubkey && newBidValue.Cmp(topBid.Value()) < 1 {
			return false, nil
		}

		// what is left is that this is a new bid with a lower value from the top builder, so we need to iterate and recalculate
	}

	if !updateImmediately {
		// get all builder's latest submission values
		keyBidValues := ds.redis.keyBlockBuilderLatestBidsValueTime(slot, parentHash, proposerPubkey)

		c := tx.HGetAll(ctx, keyBidValues)
		_, err = tx.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return false, err
		}

		bidValueMap, err := c.Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return false, err
		}

		// find bid with the highest value among all the latest bids
		topBidValue := big.NewInt(0)
		topBidBuilderPubkey := ""
		for builderPubkey, valueTime := range bidValueMap {

			splitVals := strings.Split(valueTime, "-")
			if len(splitVals) != 2 {
				continue
			}
			bidValue := splitVals[0]

			val := new(big.Int)
			val.SetString(bidValue, 10)
			// if there is a bid with higher value, no need to update top bid
			if val.Cmp(newBidValue) > 0 {
				return false, nil
			}
			if val.Cmp(topBidValue) > 0 {
				topBidValue = val
				topBidBuilderPubkey = builderPubkey
			}
		}

		if topBidBuilderPubkey == "" {
			return false, ErrFailedUpdatingTopBidNoBids
		}

		keyTopBid := ds.redis.keyCacheGetHeaderResponse(slot, parentHash, proposerPubkey)
		currentTopBidCmd := tx.Get(ctx, keyTopBid)
		_, err = tx.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			return false, err
		}
		val, err := currentTopBidCmd.Result()
		if err != nil {
			return false, err
		}
		newTopBidStr = val
	}

	if newTopBidStr == "" {
		return false, nil
	}

	// save the top bid
	err = tx.Set(ctx, keyTopBid, newTopBidStr, expiryBidCache).Err()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if _, err := tx.Exec(ctx); err != nil {
		return true, nil
	}

	return true, nil
}

// SaveBlockSubmissionTx stores getHeader and getPayload for later use, to memory and Redis.
func (ds *Datastore) SaveBlockSubmissionTx(ctx context.Context, tx redis.Pipeliner, signedBidTrace *v1.BidTrace, headerResp *common.GetHeaderResponse, payloadResp *api.VersionedExecutionPayload, blockHash string, parentHash string, proposerPubkey string) error {
	// save to memory
	bidKey := GetHeaderResponseKey{
		Slot:           signedBidTrace.Slot,
		ParentHash:     parentHash,
		ProposerPubkey: proposerPubkey,
	}

	ds.localCache.getHeaderResponsesLock.Lock()
	ds.localCache.getHeaderResponses[bidKey] = headerResp
	ds.localCache.getHeaderResponsesLock.Unlock()

	blockKey := GetPayloadResponseKey{
		Slot:           signedBidTrace.Slot,
		ProposerPubkey: "",
		BlockHash:      blockHash,
	}

	ds.localCache.getPayloadResponsesLock.Lock()
	ds.localCache.getPayloadResponses[blockKey] = &common.GetPayloadResponse{Capella: payloadResp}
	ds.localCache.getPayloadResponsesLock.Unlock()

	ds.log.Infof("saved header and payload to memory, slot: %v, blockNumber: %v, value: %s, blockHash: %v, extraData %s, parentHash: %v, proposerPublicKey %v, header: %v, UTCTime: %s", signedBidTrace.Slot, payloadResp.Capella.BlockNumber, signedBidTrace.Value.String(), blockHash, common.DecodeExtraData(payloadResp.Capella.ExtraData), parentHash, proposerPubkey, *headerResp, time.Now().UTC().String())

	return nil
}

func (ds *Datastore) saveGetHeaderResponseInListTx(ctx context.Context, tx redis.Pipeliner, slot uint64, parentHash, proposerPubkey string, headerResp *common.GetHeaderResponse) error {
	key := ds.redis.keyCacheGetHeaderResponseList(slot, parentHash, proposerPubkey)

	c := tx.Get(ctx, key)
	_, err := tx.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	value, err := c.Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("failed to getHeaderResponse list: %w", err)
	}

	var bids []common.GetHeaderResponse
	if value != "" {
		err = json.Unmarshal([]byte(value), &bids)
		if err != nil {
			return fmt.Errorf("failed to unmarshal getHeaderResponse list: %w", err)
		}
	}

	bids = append(bids, *headerResp)
	return setObjTx(ctx, tx, key, bids, expiryBidCache)
}

func (ds *Datastore) SaveBuilderBlockHash(ctx context.Context, blockHash string, builderPubkey string) error {
	key := ds.redis.keyBlockBuilderSubmission(blockHash)

	return ds.redis.setString(ctx, key, builderPubkey, 5*time.Minute)
}

func (ds *Datastore) GetBuilderBlockHash(ctx context.Context, blockHash string) (string, error) {
	key := ds.redis.keyBlockBuilderSubmission(blockHash)

	return ds.redis.getString(ctx, key)
}

func (ds *Datastore) SaveDeliveredPayloadBuilderRedis(ctx context.Context, slot uint64, blockHash string) error {
	builderPubkey, err := ds.GetBuilderBlockHash(ctx, blockHash)
	if err != nil {
		return err
	}

	key := ds.redis.keyDeliveredPayload(slot)

	return ds.redis.setString(ctx, key, builderPubkey, 7*24*time.Hour)
}

func (ds *Datastore) GetDeliveredPayload(ctx context.Context, slot uint64) (string, error) {
	key := ds.redis.keyDeliveredPayload(slot)

	return ds.redis.getString(ctx, key)
}

func (ds *Datastore) SetDemotedBuilderPubkey(ctx context.Context, builderPubkey string) error {

	key := ds.redis.keyDemotedBuilders

	return ds.redis.client.HSet(ctx, key, builderPubkey, "demoted").Err()

}

func (ds *Datastore) GetDemotedBuilderPubkeys(ctx context.Context) (*syncmap.SyncMap[string, string], error) {

	key := ds.redis.keyDemotedBuilders

	val, err := ds.redis.client.HGetAll(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	demotedBuilderPubkeys := syncmap.NewStringMapOf[string]()

	if val == nil {
		return demotedBuilderPubkeys, nil
	}

	for key, value := range val {
		demotedBuilderPubkeys.Store(key, value)
	}

	return demotedBuilderPubkeys, err
}

func (ds *Datastore) CheckDemotedBuilderPubkey(ctx context.Context, builderPubkey string) (bool, error) {

	key := ds.redis.keyDemotedBuilders

	val, err := ds.redis.client.HGet(ctx, key, builderPubkey).Result()
	if err != nil || err == redis.Nil {
		return false, err
	}

	return val != "", nil
}

func (ds *Datastore) SaveBidTrace(ctx context.Context, bidTrace *v1.BidTrace, blockHash string) error {
	key := ds.redis.keyBidTrace(blockHash)
	return ds.redis.setObj(ctx, key, bidTrace, expiryBidCache)
}

func (ds *Datastore) GetBidTrace(ctx context.Context, blockHash string) (*v1.BidTrace, error) {
	key := ds.redis.keyBidTrace(blockHash)
	res := new(v1.BidTrace)
	err := ds.redis.getObj(ctx, key, res)
	return res, err
}

func (ds *Datastore) GetBuilderLatestPayloadReceivedAt(ctx context.Context, slot uint64, builderPubkey, parentHash, proposerPubkey string) (int64, error) {
	keyLatestBidsTime := ds.redis.keyBlockBuilderLatestBidsTime(slot, parentHash, proposerPubkey)
	timestamp, err := ds.redis.client.HGet(ctx, keyLatestBidsTime, builderPubkey).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return timestamp, err
}

// value stored as comma separated value, timestamp
func (ds *Datastore) getBlockBuilderLatestBidsTimeValueTx(ctx context.Context, tx redis.Pipeliner, slot uint64, parentHash, proposerPubkey string, builderPubkey string) (string, error) {
	parentHash = strings.ToLower(parentHash)
	proposerPubkey = strings.ToLower(proposerPubkey)
	builderPubkey = strings.ToLower(builderPubkey)

	// check in Redis
	keyLatestBidsTime := ds.redis.keyBlockBuilderLatestBidsValueTime(slot, parentHash, proposerPubkey)
	c := tx.HGet(ctx, keyLatestBidsTime, builderPubkey)
	_, err := tx.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}

	value, err := c.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	if value == "" {
		return "", nil
	}
	return value, nil
}

func (ds *Datastore) RedisSubscribe(subscriptionChannelName string, msgChannel chan *redis.Message) {
	pubsub := ds.redis.client.Subscribe(context.Background(), subscriptionChannelName)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		msgChannel <- msg
	}
}

func (ds *Datastore) RedisPublish(subscriptionChannelName string, message interface{}, senderUUID string) error {
	redisMessage := &RedisMessage{
		Message:    message,
		SenderUUID: senderUUID,
	}

	return ds.redis.client.Publish(context.Background(), subscriptionChannelName, redisMessage).Err()
}

type RedisMessage struct {
	Message    interface{} `json:"message"`
	SenderUUID string      `json:"sender_uuid"`
}

func (rm *RedisMessage) MarshalBinary() ([]byte, error) {
	return json.Marshal(rm)
}

func (ds *Datastore) unmarshalRedisMessage(redisMsg *redis.Message, destination any) (string, error) {
	wrappedMessage := &RedisMessage{
		Message:    destination,
		SenderUUID: "",
	}

	err := json.Unmarshal([]byte(redisMsg.Payload), wrappedMessage)
	if err != nil {
		return "", err
	}

	return wrappedMessage.SenderUUID, nil
}

func (ds *Datastore) UnmarshalRedisMessage(redisMsg *redis.Message, destination any) (string, error) {
	reflectValue := reflect.ValueOf(destination)
	if reflectValue.Kind() != reflect.Pointer || reflectValue.IsNil() {
		return "", &json.InvalidUnmarshalError{Type: reflect.TypeOf(destination)}
	}

	return ds.unmarshalRedisMessage(redisMsg, destination)
}

func (ds *Datastore) UnmarshalRedisCapellaBlockMessage(redisMsg *redis.Message, destination *common.WrappedCapellaBuilderSubmitBlockRequest) (string, error) {
	return ds.unmarshalRedisMessage(redisMsg, destination)
}
