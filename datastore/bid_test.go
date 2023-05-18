package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/attestantio/go-builder-client/api/capella"
	"github.com/attestantio/go-builder-client/spec"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/holiman/uint256"
	"github.com/redis/go-redis/v9"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"

	capellaspec "github.com/attestantio/go-eth2-client/spec/capella"
)

const (
	blockSubmissionChannel = "blockSubmission"
)

var (
	node1UUID, _            = uuid.NewV4()
	node2UUID, _            = uuid.NewV4()
	redisBlockSubChannel    = make(chan *redis.Message)
	testBlockHashString     = "0x52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c649"
	testBuilderPubKeyString = "0x81855ad8681d0d86d1e91e00167939cb6694d2c422acd208a0072939487f6999eb9d18a44784045d87f3c67cf22746e9"
	testValueString         = "1"
	testNodeUUID            = "9e0d4175-6525-4657-9118-f0109fe27990"
	testRedisMessage        = &redis.Message{
		Channel:      blockSubmissionChannel,
		Pattern:      "",
		Payload:      "{\"message\":{\"Payload\":{\"message\":{\"slot\":\"0\",\"parent_hash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"block_hash\":\"0x52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c649\",\"builder_pubkey\":\"0x81855ad8681d0d86d1e91e00167939cb6694d2c422acd208a0072939487f6999eb9d18a44784045d87f3c67cf22746e9\",\"proposer_pubkey\":\"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"proposer_fee_recipient\":\"0x0000000000000000000000000000000000000000\",\"gas_limit\":\"0\",\"gas_used\":\"0\",\"value\":\"1\"},\"execution_payload\":{\"parent_hash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"fee_recipient\":\"0x0000000000000000000000000000000000000000\",\"state_root\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"receipts_root\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"logs_bloom\":\"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"prev_randao\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"block_number\":\"0\",\"gas_limit\":\"0\",\"gas_used\":\"0\",\"timestamp\":\"0\",\"extra_data\":\"0x\",\"base_fee_per_gas\":\"0\",\"block_hash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"transactions\":[],\"withdrawals\":[]},\"signature\":\"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\"},\"ReceiveTime\":\"0001-01-01T00:00:00Z\"},\"sender_uuid\":\"9e0d4175-6525-4657-9118-f0109fe27990\"}",
		PayloadSlice: nil,
	}
	testWrappedRedisMessage = RedisMessage{
		Message:    testRedisMessage,
		SenderUUID: testNodeUUID,
	}
	testHeaderResponse = &common.GetHeaderResponse{
		Capella: &spec.VersionedSignedBuilderBid{
			Capella: &capella.SignedBuilderBid{
				Message: &capella.BuilderBid{
					Header: &capellaspec.ExecutionPayloadHeader{
						ParentHash:       phase0.Hash32{},
						FeeRecipient:     bellatrix.ExecutionAddress{},
						StateRoot:        [32]byte{0x1},
						ReceiptsRoot:     [32]byte{0x1},
						LogsBloom:        [256]byte{0x1},
						PrevRandao:       [32]byte{0x1},
						BlockNumber:      uint64(0),
						GasLimit:         uint64(0),
						GasUsed:          uint64(0),
						Timestamp:        uint64(0),
						ExtraData:        []byte{},
						BaseFeePerGas:    [32]byte{},
						BlockHash:        phase0.Hash32{0x1},
						TransactionsRoot: phase0.Root{0x1},
						WithdrawalsRoot:  phase0.Root{0x1},
					},
					Value:  &uint256.Int{},
					Pubkey: phase0.BLSPubKey{},
				},
				Signature: phase0.BLSSignature{},
			},
			Version: consensusspec.DataVersionCapella,
		},
	}
)

func TestGetBuilderLatestPayloadReceivedAt(t *testing.T) {
	ds := setupTestDatastore(t)
	ctx := context.Background()
	slot := uint64(1)
	receivedAt := time.Now()
	blockHash := common.GenerateRandomEthHash()
	builderPubKey := common.GenerateRandomPublicKey()
	blockValue := big.NewInt(1)
	payload := common.NewCapellaBuilderSubmitBlockRequest(builderPubKey, blockHash, blockValue, time.Now())

	_, err := ds.redis.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// save this builder's latest bid
		_, err := ds.SaveLatestBuilderBidTx(ctx, pipe, slot, payload.Message.BuilderPubkey.String(),
			payload.Message.ParentHash.String(), payload.Message.ProposerPubkey.String(), receivedAt, testHeaderResponse)
		if err != nil {
			return fmt.Errorf("failed to save latest builder bid: %w", err)
		}

		return nil
	})
	assert.Nil(t, err)

	resultUnixMilli, err := ds.GetBuilderLatestPayloadReceivedAt(context.Background(), slot, builderPubKey.String(), payload.Message.ParentHash.String(), payload.Message.ProposerPubkey.String())
	assert.Equal(t, receivedAt.UnixMilli(), resultUnixMilli)
}

func TestRedisSubscribeAndPublish(t *testing.T) {
	ds := setupTestDatastore(t)
	blockHash := common.GenerateRandomEthHash()
	builderPubKey := common.GenerateRandomPublicKey()
	blockValue := big.NewInt(1)
	payload := common.NewCapellaBuilderSubmitBlockRequest(builderPubKey, blockHash, blockValue, time.Now())

	wrappedPayload := common.WrappedCapellaBuilderSubmitBlockRequest{
		Payload: payload,
	}
	go ds.RedisSubscribe(blockSubmissionChannel, redisBlockSubChannel)

	// Short pause to let subscription function complete before publishing a block
	time.Sleep(100 * time.Millisecond)

	err := ds.RedisPublish(blockSubmissionChannel, wrappedPayload, node1UUID.String())
	assert.Nil(t, err)

	// Read message from subscription channel
	redisMessage := <-redisBlockSubChannel

	result := &common.WrappedCapellaBuilderSubmitBlockRequest{}
	senderUUID, err := ds.UnmarshalRedisCapellaBlockMessage(redisMessage, result)

	assert.NoError(t, err)
	assert.Equal(t, node1UUID.String(), senderUUID)
	assert.Equal(t, blockHash.String(), result.Payload.Message.BlockHash.String())
	assert.Equal(t, builderPubKey.String(), result.Payload.Message.BuilderPubkey.String())
	assert.Equal(t, blockValue.String(), result.Payload.Message.Value.ToBig().String())
}

func TestMarshalBinary(t *testing.T) {
	expected, err := json.Marshal(testWrappedRedisMessage)
	assert.Nil(t, err)

	result, err := testWrappedRedisMessage.MarshalBinary()
	assert.Nil(t, err)

	assert.Equal(t, expected, result)
}

func TestUnmarshalRedisCapellaBlockMessage(t *testing.T) {
	ds := setupTestDatastore(t)

	result := &common.WrappedCapellaBuilderSubmitBlockRequest{}
	senderUUID, err := ds.UnmarshalRedisCapellaBlockMessage(testRedisMessage, result)
	assert.Nil(t, err)

	assert.Equal(t, testNodeUUID, senderUUID)
	assert.Equal(t, testBlockHashString, result.Payload.Message.BlockHash.String())
	assert.Equal(t, testBuilderPubKeyString, result.Payload.Message.BuilderPubkey.String())
	assert.Equal(t, testValueString, result.Payload.Message.Value.ToBig().String())
}
