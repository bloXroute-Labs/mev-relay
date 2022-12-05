package datastore

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// _HexToHash converts a hexadecimal string to an Ethereum hash
func _HexToHash(s string) (ret types.Hash) {
	err := ret.UnmarshalText([]byte(s))
	if err != nil {
		common.TestLog.Error(err, " _HexToHash: ", s)
		panic(err)
	}
	return ret
}

// _HexToAddress converts a hexadecimal string to an Ethereum address
func _HexToAddress(s string) (ret types.Address) {
	err := ret.UnmarshalText([]byte(s))
	if err != nil {
		common.TestLog.Error(err, " _HexToAddress: ", s)
		panic(err)
	}
	return ret
}

// _HexToPubkey converts a hexadecimal string to a BLS Public Key
func _HexToPubkey(s string) (ret types.PublicKey) {
	err := ret.UnmarshalText([]byte(s))
	if err != nil {
		common.TestLog.Error(err, " _HexToPubkey: ", s)
		panic(err)
	}
	return
}

func saveTestBlockSubmission(t *testing.T, datastore *Datastore, parentHash string, proposerPublicKey string, value types.U256Str, slot uint64, blockHash string) {
	bidTrace := &types.SignedBidTrace{
		Signature: types.Signature{},
		Message: &types.BidTrace{
			Slot:           slot,
			ProposerPubkey: _HexToPubkey(proposerPublicKey),
			Value:          value,
		},
	}

	getHeaderResponse := &types.GetHeaderResponse{
		Data: &types.SignedBuilderBid{
			Message: &types.BuilderBid{
				Header: &types.ExecutionPayloadHeader{
					ParentHash:   _HexToHash(parentHash),
					BlockHash:    _HexToHash(blockHash),
					FeeRecipient: _HexToAddress("0x0000000000000000000000000000000000000000"),
				},
				Value: value,
			},
		},
	}

	getPayloadResponse := &types.GetPayloadResponse{
		Data: &types.ExecutionPayload{
			BlockHash: _HexToHash(blockHash),
		},
	}

	err := datastore.SaveBlockSubmission(bidTrace, getHeaderResponse, getPayloadResponse)
	assert.Nil(t, err)
}

func setupTestDatastore(t *testing.T) *Datastore {
	t.Helper()
	var err error

	redisTestServer, err := miniredis.Run()
	require.NoError(t, err)

	redisDs, err := NewRedisCache(redisTestServer.Addr(), "", 80)
	require.NoError(t, err)

	ds, err := NewDatastore(common.TestLog, redisDs, &database.MockDB{})
	require.NoError(t, err)

	return ds
}

func TestProdProposerValidatorRegistration(t *testing.T) {
	ds := setupTestDatastore(t)

	var reg1 types.SignedValidatorRegistration
	err := copier.Copy(&reg1, &common.ValidPayloadRegisterValidator)
	require.NoError(t, err)

	key := types.NewPubkeyHex(reg1.Message.Pubkey.String())

	// Set known validator and save registration
	err = ds.redis.SetKnownValidator(key, 1)
	require.NoError(t, err)
	err = ds.redis.SetValidatorRegistration(reg1)
	require.NoError(t, err)

	// Check if validator is known
	cnt, err := ds.RefreshKnownValidators()
	require.NoError(t, err)
	require.Equal(t, 1, cnt)
	require.True(t, ds.IsKnownValidator(key))

	// Copy the original registration
	var reg2 types.SignedValidatorRegistration
	err = copier.Copy(&reg2, &reg1)
	require.NoError(t, err)
}

func TestDeleteBlockSubmission(t *testing.T) {
	ds := setupTestDatastore(t)
	blockHash0 := "0x13bc9fa2367340bd4d63e4e1544c58d303a9c06c0824a5f5e7810b10b75c61ea"
	blockHash1 := "0xb32729c107e7c5a10e93819911f2be958bd2e8735f3011192daded24389edd20"
	parentHash := "0x61223a7caf2950af3219baf91361b54808d4757fa08c00cdcc89b796f172db1a"
	pubkey := "0x98ab429cbb173ed76f2718d7ae4ab1cfe8fc36375f9d6c4618f998058d0e8b158255a4387faed53bb01cf8cb2a484a04"
	saveTestBlockSubmission(t, ds, parentHash, pubkey, types.IntToU256(1), 1, blockHash0)
	saveTestBlockSubmission(t, ds, parentHash, pubkey, types.IntToU256(2), 1, blockHash1)

	// Current highest bid should be blockHash1 at value 2
	res, err := ds.GetGetHeaderResponse(1, parentHash, pubkey)
	assert.NoError(t, err)
	assert.Equal(t, res.Data.Message.Header.BlockHash.String(), blockHash1)

	// Delete highest block hash
	err = ds.DeleteBlockSubmission(1, parentHash, pubkey, blockHash1)
	assert.NoError(t, err)

	// New highest bid sohuld be blockHash0 at value 1
	res2, err := ds.GetGetHeaderResponse(1, parentHash, pubkey)
	assert.NoError(t, err)
	assert.Equal(t, res2.Data.Message.Header.BlockHash.String(), blockHash0)
}
