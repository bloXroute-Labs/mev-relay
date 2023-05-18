package datastore

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/attestantio/go-builder-client/api"
	"github.com/attestantio/go-builder-client/api/capella"
	v1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-builder-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	capellaSpec "github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func setupTestDatastore(t *testing.T) *Datastore {
	t.Helper()
	var err error

	redisTestServer, err := miniredis.Run()
	require.NoError(t, err)

	ds, err := NewDatastore(redisTestServer.Addr(), "", 80, common.TestLog)
	require.NoError(t, err)

	return ds
}

// hexToHash converts a hexadecimal string to an Ethereum hash
func hexToHash(s string) (ret types.Hash) {
	err := ret.UnmarshalText([]byte(s))
	if err != nil {
		common.TestLog.Error(err, " hexToHash: ", s)
		panic(err)
	}
	return ret
}

// hexToAddress converts a hexadecimal string to an Ethereum address
func hexToAddress(s string) (ret types.Address) {
	err := ret.UnmarshalText([]byte(s))
	if err != nil {
		common.TestLog.Error(err, " hexToAddress: ", s)
		panic(err)
	}
	return ret
}

// hexToPubkey converts a hexadecimal string to a BLS Public Key
func hexToPubkey(s string) (ret types.PublicKey) {
	err := ret.UnmarshalText([]byte(s))
	if err != nil {
		common.TestLog.Error(err, " hexToPubkey: ", s)
		panic(err)
	}
	return
}

func saveTestBlockSubmission(t *testing.T, datastore *Datastore, parentHash string, proposerPublicKey string, value types.U256Str, slot uint64, blockHash string) {
	bidValue := uint256.MustFromBig(value.BigInt())

	bidTrace := &v1.BidTrace{
		Slot:           slot,
		ProposerPubkey: phase0.BLSPubKey(hexToPubkey(proposerPublicKey)),
		Value:          bidValue,
	}

	pubkey := bidTrace.ProposerPubkey.String()
	fmt.Println(pubkey)

	getHeaderResponse := &common.GetHeaderResponse{
		Bellatrix: nil,
		Capella: &spec.VersionedSignedBuilderBid{
			Capella: &capella.SignedBuilderBid{
				Signature: phase0.BLSSignature{},
				Message: &capella.BuilderBid{
					Header: &capellaSpec.ExecutionPayloadHeader{
						ParentHash:   phase0.Hash32(hexToHash(parentHash)),
						BlockHash:    phase0.Hash32(hexToHash(blockHash)),
						FeeRecipient: bellatrix.ExecutionAddress(hexToAddress("0x0000000000000000000000000000000000000000")),
					},
					Value: bidValue,
				},
			},
		},
	}

	getPayloadResponse := &api.VersionedExecutionPayload{
		Version: 0,
		Capella: &capellaSpec.ExecutionPayload{
			BlockHash: phase0.Hash32(hexToHash(blockHash)),
		},
	}

	p := datastore.TxPipeline()
	err := datastore.SaveBlockSubmissionTx(context.Background(), p, bidTrace, getHeaderResponse, getPayloadResponse, blockHash, parentHash, proposerPublicKey)
	require.Nil(t, err)

	_, err = p.Exec(context.Background())
	require.Nil(t, err)
}
