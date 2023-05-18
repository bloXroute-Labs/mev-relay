package common

import (
	"encoding/hex"
	"math/big"
	"strings"
	"time"

	v1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloXroute-Labs/gateway/v2/test"
	"github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/holiman/uint256"

	capellaapi "github.com/attestantio/go-builder-client/api/capella"
)

const (
	sha256HashByteLength = 32
	publicKeyByteLength  = 48
)

// DecodeExtraData returns a decoded string from block ExtraData
func DecodeExtraData(extraData types.ExtraData) string {
	decodedExtraData, err := hex.DecodeString(strings.TrimPrefix(extraData.String(), "0x"))
	if err != nil {
		return ""
	}
	return string(decodedExtraData)
}

// GenerateRandomEthHash returns a random 32-byte SHA-256 hash (block hash, tx hash, etc.)
func GenerateRandomEthHash() common.Hash {
	return common.BytesToHash(test.GenerateBytes(sha256HashByteLength))
}

// GenerateRandomPublicKey returns a random 48-byte public key
func GenerateRandomPublicKey() types.PublicKey {
	builderPubKey := types.PublicKey{}
	builderPubKey.FromSlice(test.GenerateBytes(publicKeyByteLength))
	return builderPubKey
}

// NewCapellaBuilderSubmitBlockRequest returns a Capella submit block
// request based on a provided builder pub key, block hash, and value
func NewCapellaBuilderSubmitBlockRequest(builderPubKey types.PublicKey, blockHash common.Hash, bidValue *big.Int, receiveTime time.Time) *capellaapi.SubmitBlockRequest {
	bidBlockHash := types.Hash{}
	bidBlockHash.FromSlice(blockHash.Bytes())
	value := new(uint256.Int)
	value.SetFromBig(bidValue)

	return &capellaapi.SubmitBlockRequest{
		Signature: phase0.BLSSignature{},
		Message: &v1.BidTrace{
			Slot:                 0,
			ParentHash:           phase0.Hash32{},
			BlockHash:            (phase0.Hash32)(bidBlockHash),
			BuilderPubkey:        (phase0.BLSPubKey)(builderPubKey),
			ProposerPubkey:       phase0.BLSPubKey{},
			ProposerFeeRecipient: bellatrix.ExecutionAddress{},
			GasLimit:             0,
			GasUsed:              0,
			Value:                value,
		},
		ExecutionPayload: &capella.ExecutionPayload{
			ParentHash:    phase0.Hash32{},
			FeeRecipient:  bellatrix.ExecutionAddress{},
			StateRoot:     [32]byte{},
			ReceiptsRoot:  [32]byte{},
			LogsBloom:     [256]byte{},
			PrevRandao:    [32]byte{},
			BlockNumber:   0,
			GasLimit:      0,
			GasUsed:       0,
			Timestamp:     0,
			ExtraData:     nil,
			BaseFeePerGas: [32]byte{},
			BlockHash:     (phase0.Hash32)(blockHash),
			Transactions:  nil,
			Withdrawals:   []*capella.Withdrawal{},
		},
	}
}
