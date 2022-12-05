package server

// PutRelayPayload models the payload
// for the PUT request
type PutRelayPayload struct {
	URL string `json:"url"`
}

// BidTraceJSON is a struct used for
// the relay api
type BidTraceJSON struct {
	Slot                 uint64 `json:"slot,string"`
	ParentHash           string `json:"parent_hash"`
	BlockHash            string `json:"block_hash"`
	BuilderPubkey        string `json:"builder_pubkey"`
	ProposerPubkey       string `json:"proposer_pubkey"`
	ProposerFeeRecipient string `json:"proposer_fee_recipient"`
	GasLimit             uint64 `json:"gas_limit,string"`
	GasUsed              uint64 `json:"gas_used,string"`
	Value                string `json:"value"`
	NumTx                uint64 `json:"num_tx,string"`
	BlockNumber          uint64 `json:"block_number,string"`
}

// BidTraceWithTimestampJSON is a struct used for
// the relay api
type BidTraceWithTimestampJSON struct {
	BidTraceJSON
	TimestampMs int64 `json:"timestamp_ms,string,omitempty"`
	Timestamp   int64 `json:"timestamp,string,omitempty"`
}

// DeleteBidPayload models the request payload used
// to delete bids from the datastore
type DeleteBidPayload struct {
	Slot        uint64   `json:"slot"`
	Pubkey      string   `json:"pubkey"`
	BlockHashes []string `json:"blockHashes"`
	ParentHash  string   `json:"parentHash"`
}
