package database

import (
	"database/sql"
	"time"

	"github.com/flashbots/go-boost-utils/types"
)

// BlockSubmission structs
type BlockSubmission struct {
	HeaderResponse  types.GetHeaderResponse
	PayloadResponse types.GetPayloadResponse
	Submission      types.BuilderSubmitBlockRequest
}

// GetPayloadsFilters structs
type GetPayloadsFilters struct {
	Slot          uint64
	Cursor        uint64
	Limit         uint64
	BlockHash     string
	BlockNumber   uint64
	BuilderPubkey string
}

// GetBuilderSubmissionsFilters structs
type GetBuilderSubmissionsFilters struct {
	Slot          uint64
	Limit         uint64
	BlockHash     string
	BlockNumber   uint64
	BuilderPubkey string
	// Cursor      uint64
}

func (g GetBuilderSubmissionsFilters) IsDefault() bool {
	return g.Slot == 0 && g.Limit == 100 && g.BlockHash == "" && g.BlockNumber == 0 && g.BuilderPubkey == ""
}

// ValidatorRegistrationEntry structs
type ValidatorRegistrationEntry struct {
	ID         int64     `db:"id"`
	InsertedAt time.Time `db:"inserted_at"`

	Pubkey       string `db:"pubkey"`
	FeeRecipient string `db:"fee_recipient"`
	Timestamp    uint64 `db:"timestamp"`
	GasLimit     uint64 `db:"gas_limit"`
	Signature    string `db:"signature"`
}

// ExecutionPayloadEntry structs
type ExecutionPayloadEntry struct {
	ID         int64     `db:"id"`
	InsertedAt time.Time `db:"inserted_at"`

	Slot           uint64 `db:"slot"`
	ProposerPubkey string `db:"proposer_pubkey"`
	BlockHash      string `db:"block_hash"`

	Version string `db:"version"`
	Payload string `db:"payload"`
}

// BuilderBlockSubmissionEntry structs
type BuilderBlockSubmissionEntry struct {
	ID         int64     `db:"id"`
	InsertedAt time.Time `db:"inserted_at"`

	// Delivered ExecutionPayload
	ExecutionPayloadID sql.NullInt64 `db:"execution_payload_id"`

	// Sim Result
	SimSuccess bool   `db:"sim_success"`
	SimError   string `db:"sim_error"`

	// BidTrace data
	Signature string `db:"signature"`

	Slot       uint64 `db:"slot"`
	ParentHash string `db:"parent_hash"`
	BlockHash  string `db:"block_hash"`

	BuilderPubkey        string `db:"builder_pubkey"`
	ProposerPubkey       string `db:"proposer_pubkey"`
	ProposerFeeRecipient string `db:"proposer_fee_recipient"`

	GasUsed  uint64 `db:"gas_used"`
	GasLimit uint64 `db:"gas_limit"`

	NumTx int    `db:"num_tx"`
	Value string `db:"value"`

	// Helpers
	Epoch             uint64 `db:"epoch"`
	BlockNumber       uint64 `db:"block_number"`
	WasMostProfitable bool   `db:"was_most_profitable"`
}

// DeliveredPayloadEntry structs
type DeliveredPayloadEntry struct {
	ID         int64     `db:"id"`
	InsertedAt time.Time `db:"inserted_at"`

	ExecutionPayloadID       sql.NullInt64  `db:"execution_payload_id"`
	SignedBlindedBeaconBlock sql.NullString `db:"signed_blinded_beacon_block"`

	Slot  uint64 `db:"slot"`
	Epoch uint64 `db:"epoch"`

	BuilderPubkey        string `db:"builder_pubkey"`
	ProposerPubkey       string `db:"proposer_pubkey"`
	ProposerFeeRecipient string `db:"proposer_fee_recipient"`

	ParentHash  string `db:"parent_hash"`
	BlockHash   string `db:"block_hash"`
	BlockNumber uint64 `db:"block_number"`

	GasUsed  uint64 `db:"gas_used"`
	GasLimit uint64 `db:"gas_limit"`

	NumTx int    `db:"num_tx"`
	Value string `db:"value"`
}

// NewNullInt64 func
func NewNullInt64(i int64) sql.NullInt64 {
	return sql.NullInt64{
		Int64: i,
		Valid: true,
	}
}

// NewNullString func
func NewNullString(s string) sql.NullString {
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}
