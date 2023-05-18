package datastore

import (
	"context"
	"testing"

	"github.com/flashbots/go-boost-utils/types"
	"github.com/stretchr/testify/assert"
)

func TestDeleteBlockSubmission(t *testing.T) {
	ds := setupTestDatastore(t)
	blockHash0 := "0x13bc9fa2367340bd4d63e4e1544c58d303a9c06c0824a5f5e7810b10b75c61ea"
	blockHash1 := "0xb32729c107e7c5a10e93819911f2be958bd2e8735f3011192daded24389edd20"
	parentHash := "0x61223a7caf2950af3219baf91361b54808d4757fa08c00cdcc89b796f172db1a"
	pubkey := "0x98ab429cbb173ed76f2718d7ae4ab1cfe8fc36375f9d6c4618f998058d0e8b158255a4387faed53bb01cf8cb2a484a04"
	saveTestBlockSubmission(t, ds, parentHash, pubkey, types.IntToU256(1), 1, blockHash0)
	saveTestBlockSubmission(t, ds, parentHash, pubkey, types.IntToU256(2), 1, blockHash1)

	// Current highest bid should be blockHash1 at value 2
	_, res, _, err := ds.GetGetHeaderResponse(context.Background(), 1, parentHash, pubkey)
	assert.NoError(t, err)
	assert.Equal(t, res.Capella.Capella.Message.Header.BlockHash.String(), blockHash1)
}
