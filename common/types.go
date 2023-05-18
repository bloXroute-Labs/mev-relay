package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/attestantio/go-builder-client/api"
	"github.com/attestantio/go-builder-client/api/capella"
	apiv1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-builder-client/spec"
	apiv1capella "github.com/attestantio/go-eth2-client/api/v1/capella"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	consensuscapella "github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/types"
)

var ErrUnknownNetwork = errors.New("unknown network")

// BuilderEntry represents a builder that is allowed to send blocks
// Address will be schema://hostname:port
type BuilderEntry struct {
	Address string
	Pubkey  hexutil.Bytes
	URL     *url.URL
}

// NewBuilderEntry creates a new instance based on an input string
// builderURL can be IP@PORT, PUBKEY@IP:PORT, https://IP, etc.
func NewBuilderEntry(builderURL string) (entry *BuilderEntry, err error) {
	if !strings.HasPrefix(builderURL, "http") {
		builderURL = "http://" + builderURL
	}

	parsedURL, err := url.Parse(builderURL)
	if err != nil {
		return nil, err
	}

	var pubkey hexutil.Bytes
	err = pubkey.UnmarshalText([]byte(entry.URL.User.Username()))
	if err != nil {
		return nil, err
	}

	return &BuilderEntry{
		URL:     parsedURL,
		Address: parsedURL.Scheme + "://" + parsedURL.Host,
		Pubkey:  pubkey,
	}, nil
}

var (
	EthNetworkKiln    = "kiln"
	EthNetworkRopsten = "ropsten"
	EthNetworkSepolia = "sepolia"
	EthNetworkGoerli  = "goerli"
	EthNetworkMainnet = "mainnet"

	GenesisValidatorsRootGoerli = "0x043db0d9a83813551ee2f33450d23797757d430911a9320530ad8a0eabc43efb"
	GenesisForkVersionGoerli    = "0x00001020"
	BellatrixForkVersionGoerli  = "0x02001020"

	GenesisValidatorsRootGoerliShadowFork6 = "0x6985063fa80a61a958ceeac5cf6125991ac297348e42542c85affbe9fb1c7328"
	GenesisForkVersionGoerliShadowFork6    = "0x13001035"
	BellatrixForkVersionGoerliShadowFork6  = "0x22001035"

	// https://github.com/eth-clients/eth2-networks/blob/f3ccbe0cf5798d5cd23e4e6e7119aefa043c0935/shared/mainnet/config.yaml
	GenesisValidatorsRootMainnet = "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
	GenesisForkVersionMainnet    = "0x00000000"
	BellatrixForkVersionMainnet  = "0x02000000"
)

type EpochSummary struct {
	Epoch uint64 `json:"epoch"      db:"epoch"`

	// first and last slots are just derived from the epoch
	SlotFirst uint64 `json:"slot_first" db:"slot_first"`
	SlotLast  uint64 `json:"slot_last"  db:"slot_last"`

	// registered are those that were actually used by the relay (some might be skipped if only one relay and it started in the middle of the epoch)
	SlotFirstProcessed uint64 `json:"slot_first_processed" db:"slot_first_processed"`
	SlotLastProcessed  uint64 `json:"slot_last_processed"  db:"slot_last_processed"`

	// Validator stats
	ValidatorsKnownTotal                     uint64 `json:"validators_known_total"                      db:"validators_known_total"`
	ValidatorRegistrationsTotal              uint64 `json:"validator_registrations_total"               db:"validator_registrations_total"`
	ValidatorRegistrationsSaved              uint64 `json:"validator_registrations_saved"               db:"validator_registrations_saved"`
	ValidatorRegistrationsReceivedUnverified uint64 `json:"validator_registrations_received_unverified" db:"validator_registrations_received_unverified"`

	// The number of requests are the count of all requests to a specific path, even invalid ones
	NumRegisterValidatorRequests uint64 `json:"num_register_validator_requests" db:"num_register_validator_requests"`
	NumGetHeaderRequests         uint64 `json:"num_get_header_requests"         db:"num_get_header_requests"`
	NumGetPayloadRequests        uint64 `json:"num_get_payload_requests"        db:"num_get_payload_requests"`

	// Responses to successful queries
	NumHeaderSentOk       uint64 `json:"num_header_sent_ok"       db:"num_header_sent_ok"`
	NumHeaderSent204      uint64 `json:"num_header_sent_204"      db:"num_header_sent_204"`
	NumPayloadSent        uint64 `json:"num_payload_sent"         db:"num_payload_sent"`
	NumBuilderBidReceived uint64 `json:"num_builder_bid_received" db:"num_builder_bid_received"`

	// Whether all slots were seen
	IsComplete bool `json:"is_complete" db:"is_complete"`
}

type SlotSummary struct {
	Slot   uint64 `json:"slot"   db:"slot"`
	Epoch  uint64 `json:"epoch"  db:"epoch"`
	Missed bool   `json:"missed" db:"missed"`

	// General validator stats
	ValidatorsKnownTotal        uint64 `json:"validators_known_total"                      db:"validators_known_total"`
	ValidatorRegistrationsTotal uint64 `json:"validator_registrations_total"               db:"validator_registrations_total"`

	// Slot proposer details
	ProposerPubkey       string `json:"proposer_pubkey"        db:"proposer_pubkey"`
	ProposerIsRegistered bool   `json:"proposer_is_registered" db:"proposer_is_registered"`

	// The number of requests are the count of all requests to a specific path, even invalid ones
	NumGetHeaderRequests  uint64 `json:"num_get_header_requests"         db:"num_get_header_requests"`
	NumGetPayloadRequests uint64 `json:"num_get_payload_requests"        db:"num_get_payload_requests"`

	// Responses to successful queries
	NumHeaderSentOk       uint64 `json:"num_header_sent_ok"       db:"num_header_sent_ok"`
	NumHeaderSent204      uint64 `json:"num_header_sent_204"      db:"num_header_sent_204"`
	NumPayloadSent        uint64 `json:"num_payload_sent"         db:"num_payload_sent"`
	NumBuilderBidReceived uint64 `json:"num_builder_bid_received" db:"num_builder_bid_received"`
}

// CapellaBuilderSubmitBlockRequest supports capella block submissions
type CapellaBuilderSubmitBlockRequest capella.SubmitBlockRequest

// WrappedCapellaBuilderSubmitBlockRequest supports capella block submissions
type WrappedCapellaBuilderSubmitBlockRequest struct {
	Payload     *capella.SubmitBlockRequest
	ReceiveTime time.Time
}

func (cpsbr *CapellaBuilderSubmitBlockRequest) MarshalBinary() ([]byte, error) {
	return json.Marshal(cpsbr)
}

// BellatrixBuilderSubmitBlockRequest supports bellatrix block submissions
type BellatrixBuilderSubmitBlockRequest struct {
	Signature        types.Signature         `json:"signature" ssz-size:"96"`
	Message          *types.BidTrace         `json:"message"`
	ExecutionPayload *types.ExecutionPayload `json:"execution_payload"`
}

type BuilderBlockValidationRequestV2 struct {
	capella.SubmitBlockRequest
	RegisteredGasLimit uint64         `json:"registered_gas_limit,string"`
	WithdrawalsRoot    ethcommon.Hash `json:"withdrawals_root"`
}

type BeaconBlockResponse struct {
	Data BeaconBlockData `json:"data"`
}
type BeaconBlockData struct {
	Message BeaconBlockMessage `json:"message"`
}
type BeaconBlockMessage struct {
	Body          BeaconBlock `json:"body"`
	Slot          string      `json:"slot"`
	ProposerIndex string      `json:"proposer_index"`
}

type BeaconBlock struct {
	ExecutionPayload types.ExecutionPayload `json:"execution_payload"`
	RandaoReveal     string                 `json:"randao_reveal"`
}

var (
	ErrEmptyPayload    = errors.New("empty payload")
	EthNetworkZhejiang = "zhejiang"
	EthNetworkCustom   = "custom"

	CapellaForkVersionRopsten = "0x03001020"
	CapellaForkVersionSepolia = "0x90000072"
	CapellaForkVersionGoerli  = "0x03001020"
	CapellaForkVersionMainnet = "0x03000000"

	// Zhejiang details
	GenesisForkVersionZhejiang    = "0x00000069"
	GenesisValidatorsRootZhejiang = "0x53a92d8f2bb1d85f62d16a156e6ebcd1bcaba652d0900b2c2f387826f3481f6f"
	BellatrixForkVersionZhejiang  = "0x00000071"
	CapellaForkVersionZhejiang    = "0x00000072"
)

type EthNetworkDetails struct {
	Name                     string
	GenesisForkVersionHex    string
	GenesisValidatorsRootHex string
	BellatrixForkVersionHex  string
	CapellaForkVersionHex    string

	DomainBuilder                 types.Domain
	DomainBeaconProposerBellatrix types.Domain
	DomainBeaconProposerCapella   types.Domain
}

func NewEthNetworkDetails(networkName string) (ret *EthNetworkDetails, err error) {
	var genesisForkVersion string
	var genesisValidatorsRoot string
	var bellatrixForkVersion string
	var capellaForkVersion string
	var domainBuilder types.Domain
	var domainBeaconProposerBellatrix types.Domain
	var domainBeaconProposerCapella types.Domain

	switch networkName {
	case EthNetworkRopsten:
		genesisForkVersion = types.GenesisForkVersionRopsten
		genesisValidatorsRoot = types.GenesisValidatorsRootRopsten
		bellatrixForkVersion = types.BellatrixForkVersionRopsten
		capellaForkVersion = CapellaForkVersionRopsten
	case EthNetworkSepolia:
		genesisForkVersion = types.GenesisForkVersionSepolia
		genesisValidatorsRoot = types.GenesisValidatorsRootSepolia
		bellatrixForkVersion = types.BellatrixForkVersionSepolia
		capellaForkVersion = CapellaForkVersionSepolia
	case EthNetworkGoerli:
		genesisForkVersion = types.GenesisForkVersionGoerli
		genesisValidatorsRoot = types.GenesisValidatorsRootGoerli
		bellatrixForkVersion = types.BellatrixForkVersionGoerli
		capellaForkVersion = CapellaForkVersionGoerli
	case EthNetworkMainnet:
		genesisForkVersion = types.GenesisForkVersionMainnet
		genesisValidatorsRoot = types.GenesisValidatorsRootMainnet
		bellatrixForkVersion = types.BellatrixForkVersionMainnet
		capellaForkVersion = CapellaForkVersionMainnet
	case EthNetworkZhejiang:
		genesisForkVersion = GenesisForkVersionZhejiang
		genesisValidatorsRoot = GenesisValidatorsRootZhejiang
		bellatrixForkVersion = BellatrixForkVersionZhejiang
		capellaForkVersion = CapellaForkVersionZhejiang
	case EthNetworkCustom:
		genesisForkVersion = os.Getenv("GENESIS_FORK_VERSION")
		genesisValidatorsRoot = os.Getenv("GENESIS_VALIDATORS_ROOT")
		bellatrixForkVersion = os.Getenv("BELLATRIX_FORK_VERSION")
		capellaForkVersion = os.Getenv("CAPELLA_FORK_VERSION")
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnknownNetwork, networkName)
	}

	domainBuilder, err = ComputeDomain(types.DomainTypeAppBuilder, genesisForkVersion, types.Root{}.String())
	if err != nil {
		return nil, err
	}

	domainBeaconProposerBellatrix, err = ComputeDomain(types.DomainTypeBeaconProposer, bellatrixForkVersion, genesisValidatorsRoot)
	if err != nil {
		return nil, err
	}

	domainBeaconProposerCapella, err = ComputeDomain(types.DomainTypeBeaconProposer, capellaForkVersion, genesisValidatorsRoot)
	if err != nil {
		return nil, err
	}

	return &EthNetworkDetails{
		Name:                          networkName,
		GenesisForkVersionHex:         genesisForkVersion,
		GenesisValidatorsRootHex:      genesisValidatorsRoot,
		BellatrixForkVersionHex:       bellatrixForkVersion,
		CapellaForkVersionHex:         capellaForkVersion,
		DomainBuilder:                 domainBuilder,
		DomainBeaconProposerBellatrix: domainBeaconProposerBellatrix,
		DomainBeaconProposerCapella:   domainBeaconProposerCapella,
	}, nil
}

func (e *EthNetworkDetails) String() string {
	return fmt.Sprintf("EthNetworkDetails{Name: %s, GenesisForkVersionHex: %s, GenesisValidatorsRootHex: %s, BellatrixForkVersionHex: %s, CapellaForkVersionHex: %s, DomainBuilder: %x, DomainBeaconProposerBellatrix: %x, DomainBeaconProposerCapella: %x}",
		e.Name, e.GenesisForkVersionHex, e.GenesisValidatorsRootHex, e.BellatrixForkVersionHex, e.CapellaForkVersionHex, e.DomainBuilder, e.DomainBeaconProposerBellatrix, e.DomainBeaconProposerCapella)
}

type BuilderGetValidatorsResponseEntry struct {
	Slot           uint64                             `json:"slot,string"`
	ValidatorIndex uint64                             `json:"validator_index,string"`
	Entry          *types.SignedValidatorRegistration `json:"entry"`
}

type BidTraceV2 struct {
	apiv1.BidTrace
	BlockNumber uint64 `json:"block_number,string" db:"block_number"`
	NumTx       uint64 `json:"num_tx,string" db:"num_tx"`
}

type BidTraceV2JSON struct {
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

func (b BidTraceV2) MarshalJSON() ([]byte, error) {
	return json.Marshal(&BidTraceV2JSON{
		Slot:                 b.Slot,
		ParentHash:           b.ParentHash.String(),
		BlockHash:            b.BlockHash.String(),
		BuilderPubkey:        b.BuilderPubkey.String(),
		ProposerPubkey:       b.ProposerPubkey.String(),
		ProposerFeeRecipient: b.ProposerFeeRecipient.String(),
		GasLimit:             b.GasLimit,
		GasUsed:              b.GasUsed,
		Value:                b.Value.ToBig().String(),
		NumTx:                b.NumTx,
		BlockNumber:          b.BlockNumber,
	})
}

func (b *BidTraceV2) UnmarshalJSON(data []byte) error {
	params := &struct {
		NumTx       uint64 `json:"num_tx,string"`
		BlockNumber uint64 `json:"block_number,string"`
	}{}
	err := json.Unmarshal(data, params)
	if err != nil {
		return err
	}
	b.NumTx = params.NumTx
	b.BlockNumber = params.BlockNumber

	bidTrace := new(apiv1.BidTrace)
	err = json.Unmarshal(data, bidTrace)
	if err != nil {
		return err
	}
	b.BidTrace = *bidTrace
	return nil
}

func (b *BidTraceV2JSON) CSVHeader() []string {
	return []string{
		"slot",
		"parent_hash",
		"block_hash",
		"builder_pubkey",
		"proposer_pubkey",
		"proposer_fee_recipient",
		"gas_limit",
		"gas_used",
		"value",
		"num_tx",
		"block_number",
	}
}

func (b *BidTraceV2JSON) ToCSVRecord() []string {
	return []string{
		fmt.Sprint(b.Slot),
		b.ParentHash,
		b.BlockHash,
		b.BuilderPubkey,
		b.ProposerPubkey,
		b.ProposerFeeRecipient,
		fmt.Sprint(b.GasLimit),
		fmt.Sprint(b.GasUsed),
		b.Value,
		fmt.Sprint(b.NumTx),
		fmt.Sprint(b.BlockNumber),
	}
}

type BidTraceV2WithTimestampJSON struct {
	BidTraceV2JSON
	Timestamp   int64 `json:"timestamp,string,omitempty"`
	TimestampMs int64 `json:"timestamp_ms,string,omitempty"`
}

func (b *BidTraceV2WithTimestampJSON) CSVHeader() []string {
	return []string{
		"slot",
		"parent_hash",
		"block_hash",
		"builder_pubkey",
		"proposer_pubkey",
		"proposer_fee_recipient",
		"gas_limit",
		"gas_used",
		"value",
		"num_tx",
		"block_number",
		"timestamp",
		"timestamp_ms",
	}
}

func (b *BidTraceV2WithTimestampJSON) ToCSVRecord() []string {
	return []string{
		fmt.Sprint(b.Slot),
		b.ParentHash,
		b.BlockHash,
		b.BuilderPubkey,
		b.ProposerPubkey,
		b.ProposerFeeRecipient,
		fmt.Sprint(b.GasLimit),
		fmt.Sprint(b.GasUsed),
		b.Value,
		fmt.Sprint(b.NumTx),
		fmt.Sprint(b.BlockNumber),
		fmt.Sprint(b.Timestamp),
		fmt.Sprint(b.TimestampMs),
	}
}

type SignedBlindedBeaconBlock struct {
	Bellatrix *types.SignedBlindedBeaconBlock
	Capella   *apiv1capella.SignedBlindedBeaconBlock
}

func (s *SignedBlindedBeaconBlock) MarshalJSON() ([]byte, error) {
	if s.Capella != nil {
		return json.Marshal(s.Capella)
	}
	if s.Bellatrix != nil {
		return json.Marshal(s.Bellatrix)
	}
	return nil, ErrEmptyPayload
}

func (s *SignedBlindedBeaconBlock) Slot() uint64 {
	if s.Capella != nil {
		return uint64(s.Capella.Message.Slot)
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message.Slot
	}
	return 0
}

func (s *SignedBlindedBeaconBlock) BlockHash() string {
	if s.Capella != nil {
		return s.Capella.Message.Body.ExecutionPayloadHeader.BlockHash.String()
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message.Body.ExecutionPayloadHeader.BlockHash.String()
	}
	return ""
}

func (s *SignedBlindedBeaconBlock) BlockNumber() uint64 {
	if s.Capella != nil {
		return s.Capella.Message.Body.ExecutionPayloadHeader.BlockNumber
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message.Body.ExecutionPayloadHeader.BlockNumber
	}
	return 0
}

func (s *SignedBlindedBeaconBlock) ProposerIndex() uint64 {
	if s.Capella != nil {
		return uint64(s.Capella.Message.ProposerIndex)
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message.ProposerIndex
	}
	return 0
}

func (s *SignedBlindedBeaconBlock) Signature() []byte {
	if s.Capella != nil {
		return s.Capella.Signature[:]
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Signature[:]
	}
	return nil
}

//nolint:nolintlint,ireturn
func (s *SignedBlindedBeaconBlock) Message() types.HashTreeRoot {
	if s.Capella != nil {
		return s.Capella.Message
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message
	}
	return nil
}

type SignedBeaconBlock struct {
	Bellatrix *types.SignedBeaconBlock
	Capella   *consensuscapella.SignedBeaconBlock
}

func (s *SignedBeaconBlock) MarshalJSON() ([]byte, error) {
	if s.Capella != nil {
		return json.Marshal(s.Capella)
	}
	if s.Bellatrix != nil {
		return json.Marshal(s.Bellatrix)
	}
	return nil, ErrEmptyPayload
}

func (s *SignedBeaconBlock) Slot() uint64 {
	if s.Capella != nil {
		return uint64(s.Capella.Message.Slot)
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message.Slot
	}
	return 0
}

func (s *SignedBeaconBlock) BlockHash() string {
	if s.Capella != nil {
		return s.Capella.Message.Body.ExecutionPayload.BlockHash.String()
	}
	if s.Bellatrix != nil {
		return s.Bellatrix.Message.Body.ExecutionPayload.BlockHash.String()
	}
	return ""
}

type VersionedExecutionPayload struct {
	Bellatrix *types.GetPayloadResponse
	Capella   *api.VersionedExecutionPayload
}

func (e *VersionedExecutionPayload) MarshalJSON() ([]byte, error) {
	if e.Capella != nil {
		return json.Marshal(e.Capella)
	}
	if e.Bellatrix != nil {
		return json.Marshal(e.Bellatrix)
	}

	return nil, ErrEmptyPayload
}

func (e *VersionedExecutionPayload) UnmarshalJSON(data []byte) error {
	capella := new(api.VersionedExecutionPayload)
	err := json.Unmarshal(data, capella)
	if err == nil && capella.Capella != nil {
		e.Capella = capella
		return nil
	}
	bellatrix := new(types.GetPayloadResponse)
	err = json.Unmarshal(data, bellatrix)
	if err != nil {
		return err
	}
	e.Bellatrix = bellatrix
	return nil
}

func (e *VersionedExecutionPayload) NumTx() int {
	if e.Capella != nil {
		return len(e.Capella.Capella.Transactions)
	}
	if e.Bellatrix != nil {
		return len(e.Bellatrix.Data.Transactions)
	}
	return 0
}

type BuilderSubmitBlockRequest struct {
	Bellatrix *types.BuilderSubmitBlockRequest
	Capella   *capella.SubmitBlockRequest
}

func (b *BuilderSubmitBlockRequest) MarshalJSON() ([]byte, error) {
	if b.Capella != nil {
		return json.Marshal(b.Capella)
	}
	if b.Bellatrix != nil {
		return json.Marshal(b.Bellatrix)
	}
	return nil, ErrEmptyPayload
}

func (b *BuilderSubmitBlockRequest) UnmarshalJSON(data []byte) error {
	capella := new(capella.SubmitBlockRequest)
	err := json.Unmarshal(data, capella)
	if err == nil {
		b.Capella = capella
		return nil
	}
	bellatrix := new(types.BuilderSubmitBlockRequest)
	err = json.Unmarshal(data, bellatrix)
	if err != nil {
		return err
	}
	b.Bellatrix = bellatrix
	return nil
}

func (b *BuilderSubmitBlockRequest) HasExecutionPayload() bool {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload != nil
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload != nil
	}
	return false
}

func (b *BuilderSubmitBlockRequest) ExecutionPayloadResponse() (*GetPayloadResponse, error) {
	if b.Bellatrix != nil {
		return &GetPayloadResponse{
			Bellatrix: &types.GetPayloadResponse{
				Version: types.VersionString(consensusspec.DataVersionBellatrix.String()),
				Data:    b.Bellatrix.ExecutionPayload,
			},
			Capella: nil,
		}, nil
	}

	if b.Capella != nil {
		return &GetPayloadResponse{
			Capella: &api.VersionedExecutionPayload{
				Version:   consensusspec.DataVersionCapella,
				Capella:   b.Capella.ExecutionPayload,
				Bellatrix: nil,
			},
			Bellatrix: nil,
		}, nil
	}

	return nil, ErrEmptyPayload
}

func (b *BuilderSubmitBlockRequest) Slot() uint64 {
	if b.Capella != nil {
		return b.Capella.Message.Slot
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.Message.Slot
	}
	return 0
}

func (b *BuilderSubmitBlockRequest) BlockHash() string {
	if b.Capella != nil {
		return b.Capella.Message.BlockHash.String()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.Message.BlockHash.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) ExecutionPayloadBlockHash() string {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.BlockHash.String()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.BlockHash.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) BuilderPubkey() phase0.BLSPubKey {
	if b.Capella != nil {
		return b.Capella.Message.BuilderPubkey
	}
	if b.Bellatrix != nil {
		return (phase0.BLSPubKey)(b.Bellatrix.Message.BuilderPubkey)
	}
	return phase0.BLSPubKey{}
}

func (b *BuilderSubmitBlockRequest) ProposerFeeRecipient() string {
	if b.Capella != nil {
		return b.Capella.Message.ProposerFeeRecipient.String()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.Message.ProposerFeeRecipient.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) Timestamp() uint64 {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.Timestamp
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.Timestamp
	}
	return 0
}

func (b *BuilderSubmitBlockRequest) ProposerPubkey() string {
	if b.Capella != nil {
		return b.Capella.Message.ProposerPubkey.String()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.Message.ProposerPubkey.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) ParentHash() string {
	if b.Capella != nil {
		return b.Capella.Message.ParentHash.String()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.Message.ParentHash.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) ExecutionPayloadParentHash() string {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.ParentHash.String()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.ParentHash.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) Value() *big.Int {
	if b.Capella != nil {
		return b.Capella.Message.Value.ToBig()
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.Message.Value.BigInt()
	}
	return nil
}

func (b *BuilderSubmitBlockRequest) NumTx() int {
	if b.Capella != nil {
		return len(b.Capella.ExecutionPayload.Transactions)
	}
	if b.Bellatrix != nil {
		return len(b.Bellatrix.ExecutionPayload.Transactions)
	}
	return 0
}

func (b *BuilderSubmitBlockRequest) BlockNumber() uint64 {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.BlockNumber
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.BlockNumber
	}
	return 0
}

func (b *BuilderSubmitBlockRequest) GasUsed() uint64 {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.GasUsed
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.GasUsed
	}
	return 0
}

func (b *BuilderSubmitBlockRequest) GasLimit() uint64 {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.GasLimit
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.GasLimit
	}
	return 0
}

func (b *BuilderSubmitBlockRequest) Signature() phase0.BLSSignature {
	if b.Capella != nil {
		return b.Capella.Signature
	}
	if b.Bellatrix != nil {
		return phase0.BLSSignature(b.Bellatrix.Signature)
	}
	return phase0.BLSSignature{}
}

func (b *BuilderSubmitBlockRequest) Random() string {
	if b.Capella != nil {
		return fmt.Sprintf("%#x", b.Capella.ExecutionPayload.PrevRandao)
	}
	if b.Bellatrix != nil {
		return b.Bellatrix.ExecutionPayload.Random.String()
	}
	return ""
}

func (b *BuilderSubmitBlockRequest) Message() *apiv1.BidTrace {
	if b.Capella != nil {
		return b.Capella.Message
	}
	if b.Bellatrix != nil {
		return BoostBidToBidTrace(b.Bellatrix.Message)
	}
	return nil
}

func BoostBidToBidTrace(bidTrace *types.BidTrace) *apiv1.BidTrace {
	if bidTrace == nil {
		return nil
	}
	return &apiv1.BidTrace{
		BuilderPubkey:        (phase0.BLSPubKey)(bidTrace.BuilderPubkey),
		Slot:                 bidTrace.Slot,
		ProposerPubkey:       (phase0.BLSPubKey)(bidTrace.ProposerPubkey),
		ProposerFeeRecipient: (bellatrix.ExecutionAddress)(bidTrace.ProposerFeeRecipient),
		BlockHash:            (phase0.Hash32)(bidTrace.BlockHash),
		Value:                U256StrToUint256(bidTrace.Value),
		ParentHash:           (phase0.Hash32)(bidTrace.ParentHash),
		GasLimit:             bidTrace.GasLimit,
		GasUsed:              bidTrace.GasUsed,
	}
}

type GetPayloadResponse struct {
	Bellatrix *types.GetPayloadResponse
	Capella   *api.VersionedExecutionPayload
}

func (p *GetPayloadResponse) UnmarshalJSON(data []byte) error {
	capella := new(api.VersionedExecutionPayload)
	err := json.Unmarshal(data, capella)
	if err == nil && capella.Capella != nil {
		p.Capella = capella
		return nil
	}
	bellatrix := new(types.GetPayloadResponse)
	err = json.Unmarshal(data, bellatrix)
	if err != nil {
		return err
	}
	p.Bellatrix = bellatrix
	return nil
}

func (p *GetPayloadResponse) MarshalJSON() ([]byte, error) {
	if p.Bellatrix != nil {
		return json.Marshal(p.Bellatrix)
	}
	if p.Capella != nil {
		return json.Marshal(p.Capella)
	}
	return nil, ErrEmptyPayload
}

type GetHeaderResponse struct {
	Bellatrix *types.GetHeaderResponse
	Capella   *spec.VersionedSignedBuilderBid
}

func (p *GetHeaderResponse) UnmarshalJSON(data []byte) error {
	capella := new(spec.VersionedSignedBuilderBid)
	err := json.Unmarshal(data, capella)
	if err == nil && capella.Capella != nil {
		p.Capella = capella
		return nil
	}
	bellatrix := new(types.GetHeaderResponse)
	err = json.Unmarshal(data, bellatrix)
	if err != nil {
		return err
	}
	p.Bellatrix = bellatrix
	return nil
}

func (p *GetHeaderResponse) MarshalJSON() ([]byte, error) {
	if p.Capella != nil {
		return json.Marshal(p.Capella)
	}
	if p.Bellatrix != nil {
		return json.Marshal(p.Bellatrix)
	}
	return nil, ErrEmptyPayload
}

func (p *GetHeaderResponse) Value() *big.Int {
	if p.Capella != nil {
		return p.Capella.Capella.Message.Value.ToBig()
	}
	if p.Bellatrix != nil {
		return p.Bellatrix.Data.Message.Value.BigInt()
	}
	return nil
}

func (p *GetHeaderResponse) BlockHash() phase0.Hash32 {
	if p.Capella != nil {
		return p.Capella.Capella.Message.Header.BlockHash
	}
	if p.Bellatrix != nil {
		return phase0.Hash32(p.Bellatrix.Data.Message.Header.BlockHash)
	}
	return phase0.Hash32{}
}

func (p *GetHeaderResponse) Empty() bool {
	if p == nil {
		return true
	}
	if p.Capella != nil {
		return p.Capella.Capella == nil || p.Capella.Capella.Message == nil
	}
	if p.Bellatrix != nil {
		return p.Bellatrix.Data == nil || p.Bellatrix.Data.Message == nil
	}
	return true
}

func (b *BuilderSubmitBlockRequest) Withdrawals() []*consensuscapella.Withdrawal {
	if b.Capella != nil {
		return b.Capella.ExecutionPayload.Withdrawals
	}
	return nil
}
