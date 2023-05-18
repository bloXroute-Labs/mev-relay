package server

import (
	"time"

	"github.com/attestantio/go-builder-client/api/capella"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/flashbots/go-boost-utils/types"
)

const (
	statsNameBestHeader                         = "stats.best_header"
	statsNameBestPayload                        = "stats.best_payload"
	statsNameBlockComparison                    = "stats.block_comparison"
	statsNameBlockComparisonWithProvidedHeaders = "stats.block_comparison_with_provided_headers"
	statsNameNewBlockFromBuilder                = "stats.new_block_from_builder"
	statsNamePerformance                        = "builder-relay.stats.performance"
	statsNameSlot                               = "stats.slot"
)

type builderBlockReceivedStatsRecord struct {
	BuilderIP                      string        `json:"builder_ip"`
	Slot                           uint64        `json:"slot"`
	SlotStartTime                  string        `json:"slot_start_time"`
	BlockHash                      string        `json:"block_hash"`
	ParentHash                     string        `json:"parent_hash"`
	BuilderPubkey                  string        `json:"builder_pubkey"`
	ProposerPubkey                 string        `json:"proposer_pubkey"`
	Value                          types.U256Str `json:"value"`
	ETHValue                       string        `json:"ethValue"`
	TxCount                        int           `json:"tx_count"`
	AccountID                      string        `json:"account_id"`
	FromInternalBuilder            bool          `json:"from_internal_builder"`
	SimulationTime                 string        `json:"simulation_time"`
	IsBuilderPubkeyHighPriority    bool          `json:"is_builder_pubkey_high_priority"`
	IsBuilderAccountIDHighPriority bool          `json:"is_builder_account_id_high_priority"`
	HttpResponseCode               int           `json:"http_response_code"`
	ProposerFeeRecipient           string        `json:"proposer_fee_recipient"`
	SimulationNodeURL              string        `json:"simulation_node_url"`
}

func (m *BoostService) createBuilderBlockReceivedStatsRecord(remoteAddr, externalBuilderAccountID string,
	payload *capella.SubmitBlockRequest, isExternalBuilder bool) builderBlockReceivedStatsRecord {
	isBuilderPubkeyHighPriority, _ := m.highPriorityBuilderPubkeys.Load(payload.Message.BuilderPubkey.String())
	isBuilderAccountIDHighPriority, _ := m.highPriorityBuilderAccountIDs.Load(externalBuilderAccountID)

	v := types.U256Str{}
	v.FromBig(payload.Message.Value.ToBig())

	stat := builderBlockReceivedStatsRecord{
		BuilderIP:                      remoteAddr,
		Slot:                           payload.Message.Slot,
		SlotStartTime:                  time.Unix(int64(m.genesisInfo.Data.GenesisTime+(payload.Message.Slot*12)), 0).String(),
		BlockHash:                      payload.Message.BlockHash.String(),
		ParentHash:                     payload.Message.ParentHash.String(),
		BuilderPubkey:                  payload.Message.BuilderPubkey.String(),
		ProposerPubkey:                 payload.Message.ProposerPubkey.String(),
		Value:                          v,
		ETHValue:                       common.WeiToEth(payload.Message.Value.String()),
		TxCount:                        len(payload.ExecutionPayload.Transactions),
		FromInternalBuilder:            true,
		IsBuilderPubkeyHighPriority:    isBuilderPubkeyHighPriority,
		IsBuilderAccountIDHighPriority: isBuilderAccountIDHighPriority,
		ProposerFeeRecipient:           payload.Message.ProposerFeeRecipient.String(),
	}

	if isExternalBuilder {
		stat.AccountID = externalBuilderAccountID
		stat.FromInternalBuilder = false
	}

	return stat
}

type bestHeaderStatsRecord struct {
	Data                   interface{} `json:"data"`
	Slot                   string      `json:"slot"`
	ProposerPublicKey      string      `json:"proposer_public_key"`
	Type                   string      `json:"type"`
	IsBloxrouteBlock       bool        `json:"is_bloxroute_block"`
	BestBloxrouteValue     string      `json:"best_bloxroute_value"`
	BestBloxrouteValueDiff string      `json:"best_bloxroute_value_diff"`
	BestBloxrouteBlockHash string      `json:"best_bloxroute_block_hash"`
	ExtraData              string      `json:"extra_data"`
}

type slotStatsRecord struct {
	Slot                   uint64                `json:"slot"`
	BlockHash              string                `json:"block_hash"`
	GetPayloadRequest      *requestStatsRecord   `json:"get_payload_request,omitempty"`
	GetHeaderRequest       *requestStatsRecord   `json:"get_header_request,omitempty"`
	LastRegisteredProposer *validatorStatsRecord `json:"last_registered_proposer,omitempty"`
}

type requestStatsRecord struct {
	Timestamp int64 `json:"timestamp"`
}

type validatorStatsRecord struct {
	requestStatsRecord
	ProposerPublicKey string `json:"proposer_public_key"`
}

type requestInfo struct {
	timestamp time.Time
}

type getHeaderRequestInfo struct {
	requestInfo
	pubkey    string
	blockHash string
}
