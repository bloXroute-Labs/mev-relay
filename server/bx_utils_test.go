package server

import (
	"context"
	"testing"
	"time"

	"github.com/bloXroute-Labs/mev-relay/common"

	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/stretchr/testify/assert"
)

func TestWeiToEth(t *testing.T) {
	testCasesToExpected := map[string]string{
		"":                     "0.000000000000000000",
		"1":                    "0.000000000000000001",
		"33698629863868639":    "0.033698629863868639",
		"336986298638686391":   "0.336986298638686391",
		"3369862986386863912":  "3.369862986386863912",
		"33698629863868639123": "33.698629863868639123",
	}

	for testCase, expected := range testCasesToExpected {
		result := common.WeiToEth(testCase)
		assert.Equal(t, expected, result)
	}
}

func TestSetBuilderContextForSlot(t *testing.T) {
	backend := newTestBackend(t, 1, time.Second, defaultMockDB)
	expectedContext, expectedCancelFunc := context.WithCancel(context.Background())
	newBuilderCtx := &builderContextData{Ctx: expectedContext, Cancel: expectedCancelFunc}

	backend.boost.setBuilderContextForSlot(testBuilderAPubkeyString, testSlot, newBuilderCtx)

	builderMap, found := backend.boost.builderContextsForSlot.Load(testSlot)
	assert.True(t, found)
	assert.NotNil(t, builderMap)

	resultCtx, found := builderMap.Load(testBuilderAPubkeyString)
	assert.True(t, found)
	assert.NotNil(t, resultCtx)

	assert.Equal(t, expectedContext, resultCtx.Ctx)

	// Test that expectedContext and resultContext are the same
	resultCtx.Cancel()

	select {
	case <-expectedContext.Done():
	default:
		t.Fail()
	}
}

func TestGetBuilderContextForSlot(t *testing.T) {
	backend := newTestBackend(t, 1, time.Second, defaultMockDB)
	expectedContext, expectedCancelFunc := context.WithCancel(context.Background())
	newBuilderCtx := &builderContextData{Ctx: expectedContext, Cancel: expectedCancelFunc}

	backend.boost.builderContextsForSlot = syncmap.NewIntegerMapOf[uint64, *syncmap.SyncMap[string, *builderContextData]]()
	backend.boost.setBuilderContextForSlot(testBuilderAPubkeyString, testSlot, newBuilderCtx)

	resultCtx, found := backend.boost.getBuilderContextForSlot(testBuilderAPubkeyString, testSlot)
	assert.True(t, found)
	assert.Equal(t, expectedContext, resultCtx.Ctx)

	// Test that expectedContext and resultContext are the same
	resultCtx.Cancel()

	select {
	case <-expectedContext.Done():
	default:
		t.Fail()
	}
}

func TestCancelAllBuilderContextsForSlot(t *testing.T) {
	backend := newTestBackend(t, 1, time.Second, defaultMockDB)
	expectedContext1, expectedCancelFunc1 := context.WithCancel(context.Background())
	expectedContext2, expectedCancelFunc2 := context.WithCancel(context.Background())
	newBuilderCtx1 := &builderContextData{Ctx: expectedContext1, Cancel: expectedCancelFunc1}
	newBuilderCtx2 := &builderContextData{Ctx: expectedContext2, Cancel: expectedCancelFunc2}

	backend.boost.builderContextsForSlot = syncmap.NewIntegerMapOf[uint64, *syncmap.SyncMap[string, *builderContextData]]()
	backend.boost.setBuilderContextForSlot(testBuilderAPubkeyString, testSlot, newBuilderCtx1)
	backend.boost.setBuilderContextForSlot(testBuilderBPubkeyString, testSlot, newBuilderCtx2)

	backend.boost.cancelAllBuilderContextsForSlot(testSlot)

	select {
	case <-expectedContext1.Done():
	default:
		t.Fail()
	}

	select {
	case <-expectedContext2.Done():
	default:
		t.Fail()
	}
}
