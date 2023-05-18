package database

import (
	"context"
	"time"

	"github.com/attestantio/go-builder-client/api"
	"github.com/attestantio/go-builder-client/api/capella"
	v1 "github.com/attestantio/go-builder-client/api/v1"
	capellaapi "github.com/attestantio/go-eth2-client/api/v1/capella"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/stretchr/testify/mock"
)

// MockDB struct
type MockDB struct {
	mock.Mock
}

// SaveValidatorRegistration func
func (db *MockDB) SaveValidatorRegistration(ctx context.Context, registration types.SignedValidatorRegistration) error {
	return nil
}

// SaveBuilderBlockSubmissionToDB func
func (db *MockDB) SaveBuilderBlockSubmissionToDB(ctx context.Context, payload *capella.SubmitBlockRequest, simError error, isMostProfitable bool, receivedAt time.Time) (id int64, err error) {
	return 0, nil
}

// GetExecutionPayloadEntryByID func
func (db *MockDB) GetExecutionPayloadEntryByID(ctx context.Context, executionPayloadID int64) (entry *ExecutionPayloadEntry, err error) {
	return nil, nil
}

// GetExecutionPayloadEntryBySlotPkHash func
func (db *MockDB) GetExecutionPayloadEntryBySlotPkHash(ctx context.Context, slot uint64, blockHash string) (entry *ExecutionPayloadEntry, err error) {
	return nil, nil
}

// GetBlockSubmissionEntry func
func (db *MockDB) GetBlockSubmissionEntry(ctx context.Context, slot uint64, proposerPubkey, blockHash string) (entry *BuilderBlockSubmissionEntry, err error) {
	return nil, nil
}

// GetRecentDeliveredPayloads func
func (db *MockDB) GetRecentDeliveredPayloads(filters GetPayloadsFilters) ([]*DeliveredPayloadEntry, error) {
	return nil, nil
}

// GetNumDeliveredPayloads func
func (db *MockDB) GetNumDeliveredPayloads(ctx context.Context) (uint64, error) {
	return 0, nil
}

// GetBuilderSubmissions func
func (db *MockDB) GetBuilderSubmissions(context context.Context, filters GetBuilderSubmissionsFilters) ([]*BuilderBlockSubmissionEntry, error) {
	args := db.Called(context, filters)
	return args.Get(0).([]*BuilderBlockSubmissionEntry), args.Error(1)
}

// SaveDeliveredPayload func
func (db *MockDB) SaveDeliveredPayload(slot uint64, proposerPubkey types.PubkeyHex, blockHash types.Hash, signedBlindedBeaconBlock *types.SignedBlindedBeaconBlock) error {
	return nil
}

// SaveDeliveredPayloadFromProvidedData func
func (db *MockDB) SaveDeliveredPayloadFromProvidedData(slot uint64, proposerPubkey types.PubkeyHex, blockHash types.Hash, signedBlindedBeaconBlock *capellaapi.SignedBlindedBeaconBlock, bidTrace *v1.BidTrace, getPayloadResponse *api.VersionedExecutionPayload) error {
	return nil
}

// GetBlockSubmissionEntryByHash func
func (db *MockDB) GetBlockSubmissionEntryByHash(ctx context.Context, blockHash string) (entry *BuilderBlockSubmissionEntry, err error) {
	return nil, nil
}
