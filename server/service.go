package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	capellaBuilderAPI "github.com/attestantio/go-builder-client/api/capella"
	v1 "github.com/attestantio/go-builder-client/api/v1"
	capellaapi "github.com/attestantio/go-eth2-client/api/v1/capella"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/cenkalti/backoff/v4"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	blst "github.com/supranational/blst/bindings/go"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	uberatomic "go.uber.org/atomic"

	"github.com/bloXroute-Labs/gateway/v2/services/statistics"
	"github.com/bloXroute-Labs/mev-relay/beaconclient"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/bloXroute-Labs/mev-relay/datastore"
	"go.opentelemetry.io/otel/codes"
)

const (
	defaultDatabaseRequestTimeout = 12 * time.Second

	slotsPerEpoch                        = 32
	emptyWalletAddressZero               = "0x0000000000000000000000000000000000000000"
	emptyWalletAddressOne                = "0x0000000000000000000000000000000000000001"
	coinbaseExchangeWalletAddress        = "0x4675c7e5baafbffbca748158becba61ef3b0a263"
	disableGetHeaderResponseSlotInterval = 10
	bestBlockLoggingTimerInterval        = 11 * time.Second // slot time - 1 sec
	blockSimulationTimeout               = 3 * time.Second
	builderBidsReceivedKey               = "builderBidsReceived"
	clientAllowedGetHeaderReqPerMin      = 5
	bloxrouteExtraData                   = "0x506f776572656420627920626c6f58726f757465"

	saveDeliveredPayloadMaxElapsedTime      = 20 * time.Second
	saveDeliveredPayloadMaxInterval         = 5 * time.Second
	getValidatorRegistrationsMaxElapsedTime = 2 * time.Second

	blockSubmissionChannel = "blockSubmission"
	redisPubsubChannelSize = 1000

	AuthHeaderPrefix = "bearer "
)

// RPCRequestType represents the JSON-RPC methods that are callable
type RPCRequestType string

// RPCRequestType enumeration
const (
	RPCMEVBlock     RPCRequestType = "mev_block"
	RPCProposerSlot RPCRequestType = "proposer_slot"
)

// RelayType represents the type of relay
type RelayType string

// RelayType enumeration
const (
	RelayMaxProfit RelayType = "max-profit"
	RelayRegulated RelayType = "regulated"
	RelayEthical   RelayType = "ethical"
	RelayUnknown   RelayType = ""
)

var (
	errInvalidSlot               = errors.New("invalid slot")
	errInvalidHash               = errors.New("invalid hash")
	errInvalidPubkey             = errors.New("invalid pubkey")
	errNoSuccessfulRelayResponse = errors.New("no successful relay response")
	errServerAlreadyRunning      = errors.New("server already running")
	errSimulationTimeout         = errors.New("timeout while waiting for successful simulation response")

	nilHash     = types.Hash{}
	nilResponse = struct{}{}

	defaultBuilderPubkeySkipSimulationThreshold = big.NewInt(500000000000000000) // 0.5 ETH

	defaultBuilderAccountIDSkipSimulationThreshold = new(big.Int).Mul(big.NewInt(20), big.NewInt(1000000000000000000)) // 20 ETH
)

type httpErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type blockValuePayloadParams struct {
	RelayType            RelayType `json:"relay_type"`
	SlotNumber           uint64    `json:"slot_number"`
	BlockNumber          uint64    `json:"block_number"`
	BlockHash            string    `json:"block_hash"`
	BlockValue           *big.Int  `json:"block_value"`
	ProposerFeeRecipient string    `json:"proposer_fee_recipient"`
	GasUsed              uint64    `json:"gas_used"`
	BuilderPubkey        string    `json:"builder_pubkey"`
	ParentHash           string    `json:"parent_hash"`
	TimestampMs          int64     `json:"timestamp_ms"`
}

type slotProposerPayloadParams struct {
	SlotNumber        uint64 `json:"slot_number"`
	ProposerIndex     uint64 `json:"proposer_index"`
	ProposerPublicKey string `json:"proposer_public_key"`
}

type blockValuePayload struct {
	Method string                  `json:"method"`
	Params blockValuePayloadParams `json:"params"`
}

type slotProposerPayload struct {
	Method string                    `json:"method"`
	Params slotProposerPayloadParams `json:"params"`
}

type payloadLog struct {
	BlockNumber      uint64 `json:"block_number"`
	Hash             string `json:"hash"`
	Slot             uint64 `json:"slot"`
	Proposer         uint64 `json:"proposer"`
	IsBloxrouteBlock bool   `json:"is_bloxroute_block"`
	ExtraData        string `json:"extra_data"`
}

type bidSavedLog struct {
	Duration          int64  `json:"duration"`
	ValidatorIsActive bool   `json:"validator_is_active"`
	BlockHash         string `json:"block_hash"`
	BuilderPubKey     string `json:"builder_pub_key"`
	Slot              uint64 `json:"slot"`
}

// BoostServiceOpts provides all available options for use with NewBoostService
type BoostServiceOpts struct {
	Log                         *logrus.Entry
	ListenAddr                  string
	Relays                      []RelayEntry
	GenesisForkVersionHex       string
	BellatrixForkVersionHex     string
	CapellaForkVersionHex       string
	GenesisValidatorRootHex     string
	RelayRequestTimeout         time.Duration
	RelayCheck                  bool
	MaxHeaderBytes              int
	IsRelay                     bool
	SecretKey                   *blst.SecretKey
	PubKey                      types.PublicKey
	BeaconNode                  string
	ExecutionNode               url.URL
	BeaconChain                 url.URL
	Etherscan                   url.URL
	KnownValidators             string
	CheckKnownValidators        bool
	BuilderIPs                  string
	HighPriorityBuilderPubkeys  *syncmap.SyncMap[string, bool]
	HighPerfSimBuilderPubkeys   *syncmap.SyncMap[string, bool]
	DB                          database.IDatabaseService
	RedisURI                    string
	RedisPrefix                 string
	SDNURL                      string
	CertificatesPath            string
	SimulationNodes             string
	SimulationNodeHighPerf      string
	CloudServicesEndpoint       string
	CloudServicesAuthHeader     string
	SendSlotProposerDuties      bool
	RelayType                   RelayType
	RedisPoolSize               int
	TopBlockLimit               int
	ExternalRelaysForComparison []string
	GetPayloadRequestCutoffMs   int
	GetHeaderRequestCutoffMs    int
	CapellaForkEpoch            int64
	EnableBidSaveCancellation   bool
	Tracer                      trace.Tracer

	TrustedValidatorBearerTokens *syncmap.SyncMap[string, struct{}]
}

type ProvidedHeaders struct {
	Headers           []*common.GetHeaderResponse
	ProposerPublicKey string
}

type randaoHelper struct {
	slot       uint64
	prevRandao string
}

type saveBlockData struct {
	getHeaderResponse common.GetHeaderResponse
	submission        common.WrappedCapellaBuilderSubmitBlockRequest
	signedBidTrace    *v1.BidTrace
	receivedAt        time.Time
	clientIPAddress   string
	reqRemoteAddress  string
	simulationTime    string
	tier              sdnmessage.AccountTier
}

type builderContextData struct {
	Ctx             context.Context
	Cancel          context.CancelFunc
	BidReceivedTime time.Time
	BidValue        *big.Int
	BlockHash       string
}

// BoostService TODO
type BoostService struct {
	listenAddr                     string
	relays                         []RelayEntry
	log                            *logrus.Entry
	srv                            *http.Server
	relayCheck                     bool
	db                             database.IDatabaseService
	datastore                      *datastore.Datastore
	bidTraceWithTimestampJSONCache *cache.Cache

	isRelay   bool
	secretKey *blst.SecretKey
	pubKey    types.PublicKey

	beaconClient beaconclient.MultiBeaconClient
	beaconNode,
	executionNode,
	beaconChain,
	etherscan url.URL

	knownValidators                                        string
	checkKnownValidators                                   bool
	validators                                             *syncmap.SyncMap[string, Validator]
	validatorsByIndex                                      *syncmap.SyncMap[string, string]
	validatorsLock                                         sync.RWMutex
	relayRankings                                          *syncmap.SyncMap[string, relayRanking]
	validatorActivity                                      *syncmap.SyncMap[string, validatorActivity]
	genesisForkVersion                                     string
	bellatrixForkVersion                                   string
	proposerSigningDomain                                  types.Domain
	genesisValidatorRootHex                                string
	recentBlocks                                           []blockStat
	recentBlocksLock                                       sync.RWMutex
	stats                                                  statistics.FluentdStats
	builderIPs                                             string
	highPriorityBuilderDataLock                            sync.RWMutex
	highPriorityBuilderPubkeys                             *syncmap.SyncMap[string, bool]
	highPriorityBuilderAccountIDs                          *syncmap.SyncMap[string, bool]
	highPriorityBuilderPubkeysToSkipSimulationThreshold    *syncmap.SyncMap[string, *big.Int]
	highPriorityBuilderAccountIDsToSkipSimulationThreshold *syncmap.SyncMap[string, *big.Int]
	ultraBuilderAccountIDs                                 *syncmap.SyncMap[string, bool]
	noRateLimitUltraBuilderAccIDs                          *syncmap.SyncMap[string, bool]
	highPerfSimBuilderPubkeys                              *syncmap.SyncMap[string, bool]
	certificatesPath                                       string
	sdnURL                                                 string
	simulationNodes                                        []string
	simulationNodeHighPerf                                 string

	expectedBlockNumber uint64

	expectedPrevRandao      randaoHelper
	expectedPayloadDataLock sync.RWMutex

	externalRelaysForComparison []string

	demotedBuilderLock sync.RWMutex
	demotedBuilders    *syncmap.SyncMap[string, string]

	providedHeaders *syncmap.SyncMap[int64, ProvidedHeaders]
	providedPayload *syncmap.SyncMap[int64, bool]

	maxHeaderBytes int

	builderSigningDomain    types.Domain
	httpClient              http.Client
	cloudServicesHttpClient http.Client

	genesisInfo GetGenesisResponse

	topBlockMap *syncmap.SyncMap[uint64, *syncmap.SyncMap[string, int]]

	getPayloadRequestCutoffMs int
	getHeaderRequestCutoffMs  int

	performanceStats PerformanceStats

	proposerDutiesLock           sync.RWMutex
	proposerDutiesResponse       []types.BuilderGetValidatorsResponseEntry
	proposerDutiesSlot           uint64
	proposerDutiesMap            *syncmap.SyncMap[uint64, *types.RegisterValidatorRequestMessage]
	isUpdatingProposerDuties     *uberatomic.Bool
	latestFetchedSlotNumber      uint64
	cloudServicesEndpoint        string
	cloudServicesAuthHeader      string
	sendSlotProposerDuties       bool
	relayType                    RelayType
	allProposerDuties            *syncmap.SyncMap[uint64, beaconclient.ProposerDutiesResponseData]
	allProposerDutiesLock        sync.Mutex
	latestSlotBlockReceived      *uberatomic.Uint64
	getHeaderLatestDisabledSlot  *uberatomic.Uint64
	slotStartTimes               *syncmap.SyncMap[uint64, time.Time]
	getPayloadRequests           *syncmap.SyncMap[uint64, requestInfo]
	getHeaderRequests            *syncmap.SyncMap[uint64, []getHeaderRequestInfo]
	bestBlockBidForSlot          *syncmap.SyncMap[uint64, v1.BidTrace]
	bestBlockLoggingTimer        *time.Timer
	bestBloxrouteBlockBidForSlot *syncmap.SyncMap[uint64, v1.BidTrace]

	nextSlotWithdrawalsRoot phase0.Root
	nextSlotWithdrawals     []*capella.Withdrawal
	nextSlotWithdrawalsLock sync.RWMutex

	// TODO: remove this and flag??
	capellaForkEpoch int64

	newBlockChannel                 chan *saveBlockData
	newPayloadAttributesSlotChannel chan uint64
	redisBlockSubChannel            chan *redis.Message
	builderContextsForSlot          *syncmap.SyncMap[uint64, *syncmap.SyncMap[string, *builderContextData]]
	nodeUUID                        uuid.UUID
	enableBidSaveCancellation       bool

	slotKeysToExpire             *syncmap.SyncMap[uint64, *syncmap.SyncMap[string, bool]]
	keysToExpireChan             chan *syncmap.SyncMap[uint64, []string]
	trustedValidatorBearerTokens *syncmap.SyncMap[string, struct{}]

	tracer trace.Tracer

	auth Auth
}

type deliveredPayloadsResult struct {
	res []*database.BuilderBlockSubmissionEntry
	err error
}

// NewBoostService created a new BoostService
func NewBoostService(opts BoostServiceOpts) (*BoostService, error) {
	if len(opts.Relays) == 0 {
		return nil, errors.New("no relays")
	}

	builderSigningDomain, err := ComputeDomain(types.DomainTypeAppBuilder, opts.GenesisForkVersionHex, types.Root{}.String())
	if err != nil {
		return nil, err
	}

	proposerSigningDomain, err := ComputeDomain(types.DomainTypeBeaconProposer, opts.CapellaForkVersionHex, opts.GenesisValidatorRootHex)
	if err != nil {
		return nil, err
	}

	opts.Log.Info("Setting up datastore...")
	datastoreInstance, err := datastore.NewDatastore(opts.RedisURI, opts.RedisPrefix, opts.RedisPoolSize, opts.Log)
	if err != nil {
		opts.Log.WithError(err).Fatalf("Failed setting up prod datastore")
	}

	opts.Log.Infof("Using beacon endpoints: %s", opts.BeaconNode)
	var beaconInstances []beaconclient.IBeaconInstance
	for _, uri := range strings.Split(opts.BeaconNode, ",") {
		beaconInstances = append(beaconInstances, beaconclient.NewProdBeaconInstance(opts.Log, uri))
	}

	beaconNodeURL, err := url.Parse(strings.Split(opts.BeaconNode, ",")[0])
	if err != nil {
		opts.Log.WithError(err).Error("could not process first beacon node")
	}

	boostService := &BoostService{
		listenAddr:              opts.ListenAddr,
		relays:                  opts.Relays,
		log:                     opts.Log.WithField("module", "service"),
		relayCheck:              opts.RelayCheck,
		db:                      opts.DB,
		datastore:               datastoreInstance,
		maxHeaderBytes:          opts.MaxHeaderBytes,
		isRelay:                 opts.IsRelay,
		secretKey:               opts.SecretKey,
		pubKey:                  opts.PubKey,
		executionNode:           opts.ExecutionNode,
		beaconChain:             opts.BeaconChain,
		etherscan:               opts.Etherscan,
		builderSigningDomain:    builderSigningDomain,
		knownValidators:         opts.KnownValidators,
		checkKnownValidators:    opts.CheckKnownValidators,
		certificatesPath:        opts.CertificatesPath,
		sdnURL:                  opts.SDNURL,
		validators:              syncmap.NewStringMapOf[Validator](),
		validatorsByIndex:       syncmap.NewStringMapOf[string](),
		validatorsLock:          sync.RWMutex{},
		validatorActivity:       syncmap.NewStringMapOf[validatorActivity](),
		relayRankings:           syncmap.NewStringMapOf[relayRanking](),
		genesisForkVersion:      opts.GenesisForkVersionHex,
		bellatrixForkVersion:    opts.BellatrixForkVersionHex,
		proposerSigningDomain:   proposerSigningDomain,
		genesisValidatorRootHex: opts.GenesisValidatorRootHex,
		recentBlocks:            []blockStat{},
		simulationNodes:         strings.Split(opts.SimulationNodes, ","),
		simulationNodeHighPerf:  opts.SimulationNodeHighPerf,
		beaconNode:              *beaconNodeURL,

		nextSlotWithdrawalsRoot: phase0.Root{},
		nextSlotWithdrawals:     []*capella.Withdrawal{},
		nextSlotWithdrawalsLock: sync.RWMutex{},

		expectedBlockNumber: 0,

		expectedPrevRandao:      randaoHelper{},
		expectedPayloadDataLock: sync.RWMutex{},

		demotedBuilderLock: sync.RWMutex{},
		demotedBuilders:    syncmap.NewStringMapOf[string](),

		topBlockMap: syncmap.NewIntegerMapOf[uint64, *syncmap.SyncMap[string, int]](),

		getPayloadRequestCutoffMs: opts.GetPayloadRequestCutoffMs,
		getHeaderRequestCutoffMs:  opts.GetHeaderRequestCutoffMs,

		externalRelaysForComparison: opts.ExternalRelaysForComparison,

		beaconClient:                   *beaconclient.NewMultiBeaconClient(opts.Log, beaconInstances),
		bidTraceWithTimestampJSONCache: cache.New(time.Minute*30, time.Minute*30),
		// TODO: possibly change providedHeaders back to hashmap.New[int64, []*types.GetHeaderResponse] after builder 'getValidators' implemented
		providedHeaders:               syncmap.NewIntegerMapOf[int64, ProvidedHeaders](),
		providedPayload:               syncmap.NewIntegerMapOf[int64, bool](),
		builderIPs:                    opts.BuilderIPs,
		highPriorityBuilderDataLock:   sync.RWMutex{},
		highPriorityBuilderPubkeys:    opts.HighPriorityBuilderPubkeys,
		highPerfSimBuilderPubkeys:     opts.HighPerfSimBuilderPubkeys,
		highPriorityBuilderAccountIDs: syncmap.NewStringMapOf[bool](),
		highPriorityBuilderPubkeysToSkipSimulationThreshold:    syncmap.NewStringMapOf[*big.Int](),
		highPriorityBuilderAccountIDsToSkipSimulationThreshold: syncmap.NewStringMapOf[*big.Int](),
		ultraBuilderAccountIDs:                                 syncmap.NewStringMapOf[bool](),
		noRateLimitUltraBuilderAccIDs:                          syncmap.NewStringMapOf[bool](),
		httpClient: http.Client{
			Timeout: opts.RelayRequestTimeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		cloudServicesHttpClient: http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: opts.RelayRequestTimeout,
		},
		performanceStats:                NewPerformanceStats(),
		proposerDutiesLock:              sync.RWMutex{}, // sync.RWMutex
		proposerDutiesResponse:          make([]types.BuilderGetValidatorsResponseEntry, 0),
		proposerDutiesSlot:              0, // uint64
		proposerDutiesMap:               syncmap.NewIntegerMapOf[uint64, *types.RegisterValidatorRequestMessage](),
		isUpdatingProposerDuties:        uberatomic.NewBool(false), // uberatomic.Bool
		latestFetchedSlotNumber:         0,
		cloudServicesEndpoint:           opts.CloudServicesEndpoint,
		cloudServicesAuthHeader:         opts.CloudServicesAuthHeader,
		sendSlotProposerDuties:          opts.SendSlotProposerDuties,
		relayType:                       opts.RelayType,
		allProposerDuties:               syncmap.NewIntegerMapOf[uint64, beaconclient.ProposerDutiesResponseData](),
		allProposerDutiesLock:           sync.Mutex{},
		latestSlotBlockReceived:         uberatomic.NewUint64(0),
		getHeaderLatestDisabledSlot:     uberatomic.NewUint64(0),
		slotStartTimes:                  syncmap.NewIntegerMapOf[uint64, time.Time](),
		getPayloadRequests:              syncmap.NewIntegerMapOf[uint64, requestInfo](),
		getHeaderRequests:               syncmap.NewIntegerMapOf[uint64, []getHeaderRequestInfo](),
		bestBlockBidForSlot:             syncmap.NewIntegerMapOf[uint64, v1.BidTrace](),
		bestBloxrouteBlockBidForSlot:    syncmap.NewIntegerMapOf[uint64, v1.BidTrace](),
		capellaForkEpoch:                opts.CapellaForkEpoch,
		newBlockChannel:                 make(chan *saveBlockData, redisPubsubChannelSize),
		newPayloadAttributesSlotChannel: make(chan uint64, redisPubsubChannelSize),
		redisBlockSubChannel:            make(chan *redis.Message, redisPubsubChannelSize),
		builderContextsForSlot:          syncmap.NewIntegerMapOf[uint64, *syncmap.SyncMap[string, *builderContextData]](),
		enableBidSaveCancellation:       opts.EnableBidSaveCancellation,

		slotKeysToExpire:             syncmap.NewIntegerMapOf[uint64, *syncmap.SyncMap[string, bool]](),
		keysToExpireChan:             make(chan *syncmap.SyncMap[uint64, []string]),
		trustedValidatorBearerTokens: opts.TrustedValidatorBearerTokens,

		tracer: opts.Tracer,
	}

	boostService.nodeUUID, err = uuid.NewV4()
	if err != nil {
		boostService.nodeUUID = uuid.UUID{}
		boostService.log.Error("could not create UUID for this relay node, setting to empty", "error", err)
	}

	getGenesisResponse, err := boostService.GetGenesis()
	if err != nil {
		boostService.log.Error("could not fetch genesis", "error", err)
	}

	boostService.genesisInfo = *getGenesisResponse

	go func() {
		for {
			keysToExpire := <-boostService.keysToExpireChan
			keysToExpire.Range(func(slot uint64, keys []string) bool {
				slotKeyMap, ok := boostService.slotKeysToExpire.Load(slot)
				if !ok {
					slotKeyMap = syncmap.NewStringMapOf[bool]()
				}
				for _, key := range keys {
					slotKeyMap.Store(key, true)
				}
				boostService.slotKeysToExpire.Store(slot, slotKeyMap)
				return true
			})
		}
	}()

	return boostService, nil
}

func (m *BoostService) respondErrorWithLog(w http.ResponseWriter, code int, message string, log *logrus.Entry, logMessage string) {
	log = log.WithFields(logrus.Fields{"resp_message": message, "resp_code": code})

	// write either specific error message or use one for the response
	if logMessage != "" {
		log.Error(logMessage)
	} else {
		log.Error(message)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	resp := httpErrorResp{Code: code, Message: message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.WithError(err).Error("couldn't write error response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (m *BoostService) respondError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)

	resp := httpErrorResp{Code: code, Message: message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		m.log.WithError(err).Error("couldn't write error response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (m *BoostService) respondOK(w http.ResponseWriter, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		m.log.WithField("response", response).WithError(err).Error("Couldn't write OK response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (m *BoostService) respondEncodedJSON(w http.ResponseWriter, response []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func (m *BoostService) getRouter() http.Handler {
	auth := NewAuth(m.log, m.certificatesPath, m.sdnURL, m.builderIPs)
	m.auth = auth

	r := mux.NewRouter()

	r.HandleFunc("/", m.handleIndex)
	r.PathPrefix("/static/").Handler(
		func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("Cache-Control", "max-age=18000, public, must-revalidate, proxy-revalidate")
				next.ServeHTTP(w, r)
			})
		}(http.StripPrefix("/static/", http.FileServer(http.Dir("./static")))))

	r.HandleFunc(pathStatus, m.handleStatus).Methods(http.MethodGet)
	r.HandleFunc(pathRegisterValidator, m.handleRegisterValidator).Methods(http.MethodPost)
	r.Handle(pathGetHeader, alice.New(rateLimitMiddleware(m.log, newRateLimiter(time.Minute, clientAllowedGetHeaderReqPerMin), getHeaderCaller)).
		ThenFunc(m.handleGetHeader)).Methods(http.MethodGet)

	r.HandleFunc(pathGetPayload, m.handleGetPayload).Methods(http.MethodPost)
	r.HandleFunc(pathPutRelay, m.authMiddleware(m.handlePutRelay)).Methods(http.MethodPut)

	r.Handle(pathSubmitNewBlock, alice.New(auth.whitelistIPMiddleware, auth.authMiddleware,
		m.tierRateLimitMiddleware(newTierRateLimiters())).
		ThenFunc(m.handlePostBlock)).Methods(http.MethodPost)

	r.HandleFunc(pathBuilderGetValidators, m.handleBuilderGetValidators).Methods(http.MethodGet)

	r.HandleFunc(pathDataProposerPayloadDelivered, m.handleDataProposerPayloadDelivered).Methods(http.MethodGet)
	r.HandleFunc(pathDataBuilderBidsReceived, m.handleDataBuilderBidsReceived).Methods(http.MethodGet)
	r.HandleFunc(pathDataValidatorRegistration, m.handleDataValidatorRegistration).Methods(http.MethodGet)

	r.Handle(pathBuilderDeleteBlocks, alice.New(auth.whitelistIPMiddleware).ThenFunc(m.handleBuilderDeleteBlocks)).Methods(http.MethodPost)
	r.Handle(pathBuilderDisableGetHeaderResponse, alice.New(auth.whitelistIPMiddleware).ThenFunc(m.handleBuilderDisableGetHeaderResponse)).Methods(http.MethodPost)

	r.HandleFunc(pathGetActiveValidators, m.handleActiveValidators).Methods(http.MethodGet)
	r.Handle(pathWebsocket, alice.New(auth.whitelistIPMiddleware, auth.authMiddleware).ThenFunc(m.HandleSocketConnection)).Methods(http.MethodGet)

	r.Use(otelmux.Middleware("http-server"))

	r.Use(mux.CORSMethodMiddleware(r))
	r.Use(LogRequestID(m))
	r.Use(CheckKnownValidator(m))

	return recoveryMiddlewareWithLogs(m.log, r)
}

// StartHTTPServer starts the HTTP server for this boost service instance
func (m *BoostService) StartHTTPServer() error {
	if m.srv != nil {
		return errServerAlreadyRunning
	}

	m.srv = &http.Server{
		Addr:    m.listenAddr,
		Handler: otelhttp.NewHandler(m.getRouter(), "otel-http-handler"),

		ReadTimeout:       0,
		ReadHeaderTimeout: 0,
		WriteTimeout:      0,
		IdleTimeout:       10 * time.Second,
		MaxHeaderBytes:    m.maxHeaderBytes,
	}

	err := m.srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

//lint:ignore U1000 keep for future merging
func (m *BoostService) handleRoot(w http.ResponseWriter, _ *http.Request) {
	m.respondOK(w, nilResponse)
}

// handleStatus sends calls to the status endpoint of every relay.
// It returns OK if at least one returned OK, and returns error otherwise.
func (m *BoostService) handleStatus(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathStatus, uint64(time.Since(start).Microseconds()), success)
	}()
	if !m.relayCheck {
		success = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
		return
	}

	// If relayCheck is enabled, make sure at least 1 relay returns success
	var wg sync.WaitGroup
	var numSuccessRequestsToRelay uint32
	ua := UserAgent(req.Header.Get("User-Agent"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := m.log.WithFields(logrus.Fields{
		"method": "status",
	})

	for _, r := range m.relays {
		wg.Add(1)

		go func(relay RelayEntry, log logrus.Entry) {
			defer wg.Done()
			uri := relay.GetURI(pathStatus)
			logR := log.WithField("uri", uri)
			logR.Trace("checking relay status")

			_, err := SendHTTPRequest(ctx, m.httpClient, http.MethodGet, uri, ua, nil, nil)
			if err != nil && ctx.Err() != context.Canceled {
				logR.WithError(err).Error("failed to retrieve relay status")
				return
			}

			// Success: increase counter and cancel all pending requests to other relays
			atomic.AddUint32(&numSuccessRequestsToRelay, 1)
			cancel()
		}(r, *log)
	}

	// At the end, wait for every routine and return status according to relay's ones.
	wg.Wait()

	if numSuccessRequestsToRelay > 0 {
		success = true
		m.respondOK(w, nilResponse)
	} else {
		m.respondErrorWithLog(w, http.StatusServiceUnavailable, "all relays are unavailable", log, "")
	}
}

// RegisterValidatorV1 - returns 200 if at least one relay returns 200
func (m *BoostService) handlePutRelay(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathPutRelay, uint64(time.Since(start).Microseconds()), success)
	}()

	log := m.log.WithFields(logrus.Fields{
		"method":       "putRelay",
		"requester-ip": req.RemoteAddr,
	})

	var payload PutRelayPayload
	if err := decodeJSON(req.Body, &payload); err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log.WithError(err), "failed to decode request payload")
		return
	}
	urls := m.parseRelayURLs(payload.URL)
	for _, relay := range m.relays {
		if urls[0].URL.String() == relay.URL.String() {
			m.respondErrorWithLog(w, http.StatusBadRequest, "relay already known", log, "")
			return
		}
	}
	m.relays = append(m.relays, urls...)

	success = true
	m.respondOK(w, nilResponse)
}

// RegisterValidatorV1 - returns 200 if at least one relay returns 200
func (m *BoostService) handleRegisterValidator(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathRegisterValidator, uint64(time.Since(start).Microseconds()), success)
	}()

	span := trace.SpanFromContext(req.Context())

	log := m.log.WithFields(logrus.Fields{
		"method": "registerValidator",
	})

	var (
		isTrustedValidator bool
	)
	authHeader := strings.ToLower(req.Header.Get("authorization"))
	if strings.HasPrefix(authHeader, AuthHeaderPrefix) {
		token := strings.TrimPrefix(authHeader, AuthHeaderPrefix)
		isTrustedValidator = m.trustedValidatorBearerTokens.Has(token)
	}

	var registrations []types.SignedValidatorRegistration
	if err := decodeJSONAndClose(req.Body, &registrations); err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log.WithError(err), "failed to decode request registrations")
		return
	}

	log = log.WithFields(logrus.Fields{
		"numRegistrations":   len(registrations),
		"isTrustedValidator": isTrustedValidator,
	})
	span.SetAttributes(
		attribute.String("method", "registerValidator"),
		attribute.Int("numRegistrations", len(registrations)),
		attribute.Bool("isTrustedValidator", isTrustedValidator),
	)
	spanContext := trace.ContextWithSpan(req.Context(), span)

	var registrationsPubKeys []string
	for i := range registrations {
		if registrations[i].Message == nil {
			log.Debug("registration message is nil")
			continue
		}
		registrationsPubKeys = append(registrationsPubKeys, registrations[i].Message.Pubkey.PubkeyHex().String())
	}

	storedValidatorRegistrations, err := m.datastore.GetValidatorRegistrations(spanContext, registrationsPubKeys)
	if err != nil {
		// even in case of error here, all the validators will be re-registered
		log.WithError(err).Error("failed to get validator registrations")
	}

	storedRegistrations := make(map[string]types.SignedValidatorRegistration)
	for i := range storedValidatorRegistrations {
		storedRegistrations[storedValidatorRegistrations[i].Message.Pubkey.PubkeyHex().String()] = storedValidatorRegistrations[i]
	}

	validators := make(map[string]interface{})
	activeValidators := make(map[string]interface{})

	var validRegistrations []types.SignedValidatorRegistration

	for i := range registrations {
		if registrations[i].Message == nil {
			log.Debug("registration message is nil")
			continue
		}

		// check for a previous registration fee recipient
		storedValidatorRegistration, ok := storedRegistrations[registrations[i].Message.Pubkey.PubkeyHex().String()]

		// skip signature verification if the fee recipient is the same
		if ok && storedValidatorRegistration.Message.FeeRecipient.String() == registrations[i].Message.FeeRecipient.String() {
			log.Tracef("validator public key %s and fee recipient %s validated from the cache",
				storedValidatorRegistration.Message.Pubkey.String(), registrations[i].Message.FeeRecipient.String())

			validRegistrations = append(validRegistrations, registrations[i])
			activeValidators[registrations[i].Message.Pubkey.PubkeyHex().String()] = &datastore.ValidatorLatency{
				Registration:   registrations[i],
				LastRegistered: time.Now().UnixNano(),
				IsTrusted:      isTrustedValidator,
			}
			continue
		}

		ok, err := types.VerifySignature(registrations[i].Message, m.builderSigningDomain,
			registrations[i].Message.Pubkey[:], registrations[i].Signature[:])
		if !ok || err != nil {
			if err != nil {
				log = log.WithError(err)
			}
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid signature", log, "")
			return
		} else {
			validRegistrations = append(validRegistrations, registrations[i])
			activeValidators[registrations[i].Message.Pubkey.PubkeyHex().String()] = &datastore.ValidatorLatency{
				Registration:   registrations[i],
				LastRegistered: time.Now().UnixNano(),
				IsTrusted:      isTrustedValidator,
			}
		}
	}
	if len(activeValidators) > 0 {
		go func() {
			err := m.datastore.SetValidatorRegistrationMap(spanContext, activeValidators)
			if err != nil {
				log.WithError(err).Error("failed to set validator registration")
			} else {
				log.WithField("validator-count", len(validators)).Info("saved registration")
			}
		}()
	}
	go func() {
		for _, registration := range validRegistrations {
			go func(reg types.SignedValidatorRegistration) {
				ctx, cancel := context.WithTimeout(spanContext, defaultDatabaseRequestTimeout)
				defer cancel()

				if err := m.db.SaveValidatorRegistration(ctx, reg); err != nil {
					log.WithError(err).WithField("pubkey", reg.Message.Pubkey.String()).Error("could not save registration for validator")
				}
			}(registration)
		}
	}()

	log.WithField("timeNeededSec", time.Since(start).Seconds()).Info("register validator")

	success = true
	m.respondOK(w, nilResponse)
}

// GetHeaderV1 TODO
func (m *BoostService) handleGetHeader(w http.ResponseWriter, req *http.Request) {
	start := time.Now().UTC()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathGetHeaderPrefix, uint64(time.Since(start).Microseconds()), success)
	}()

	vars := mux.Vars(req)
	parentHashHex := vars["parent_hash"]
	pubkey := vars["pubkey"]
	log := m.log.WithFields(logrus.Fields{
		"method":     "getHeader",
		"slot":       vars["slot"],
		"parentHash": parentHashHex,
		"pubkey":     pubkey,
	})

	span := trace.SpanFromContext(req.Context())
	span.SetAttributes(
		attribute.String("method", "getHeader"),
		attribute.String("slot", vars["slot"]),
		attribute.String("parentHash", parentHashHex),
		attribute.String("pubkey", pubkey),
	)

	spanContext := trace.ContextWithSpan(req.Context(), span)

	slot, err := strconv.ParseUint(vars["slot"], 10, 64)
	if err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, errInvalidSlot.Error(), log.WithError(err), "")
		return
	}

	slotStartTimestamp := m.genesisInfo.Data.GenesisTime + (slot * 12)
	msIntoSlot := start.UnixMilli() - int64(slotStartTimestamp*1000)

	if m.getHeaderRequestCutoffMs > 0 && msIntoSlot > int64(m.getHeaderRequestCutoffMs) {
		log.Info("too late into slot")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if len(pubkey) != 98 {
		m.respondErrorWithLog(w, http.StatusBadRequest, errInvalidPubkey.Error(), log, fmt.Sprintf("pub key should be %d long", 98))
		return
	}

	if len(parentHashHex) != 66 {
		m.respondErrorWithLog(w, http.StatusBadRequest, errInvalidHash.Error(), log, fmt.Sprintf("parent hash hex should be %d long", 66))
		return
	}

	if slot <= m.getHeaderLatestDisabledSlot.Load() {
		log.Errorf("no header available, 'getHeader' requests disabled through slot %v, requested slot is %v", m.getHeaderLatestDisabledSlot.Load(), slot)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	highestRankedRelay := new(string)

	fetchGetHeaderStartTime := time.Now().UTC()
	slotBestHeaderRaw, slotBestHeader, dataSource, err := m.datastore.GetGetHeaderResponse(spanContext, slot, parentHashHex, pubkey)
	if slotBestHeader == nil || err != nil {
		log.Errorf("no header available in datastore for slot: %v, parent hash: %v, proposer public key: %v, error: %v", slot, parentHashHex, pubkey, err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if slotBestHeader.Capella == nil || slotBestHeader.Capella.Capella == nil || slotBestHeader.Capella.Capella.Message == nil || slotBestHeader.Capella.Capella.Message.Header == nil || slotBestHeader.Capella.Capella.Message.Header.BlockHash.String() == nilHash.String() {
		log.Error("no bids received from relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	fetchGetHeaderDurationMS := time.Since(fetchGetHeaderStartTime).Milliseconds()

	log = log.WithFields(logrus.Fields{
		"bid-header-withdrawalroot": slotBestHeader.Capella.Capella.Message.Header.WithdrawalsRoot.String(),
		"bid-header-parenthash":     slotBestHeader.Capella.Capella.Message.Header.ParentHash.String(),
		"bid-header-blockhash":      slotBestHeader.Capella.Capella.Message.Header.BlockHash.String(),
		"bid-value":                 common.WeiToEth(slotBestHeader.Capella.Capella.Message.Value.ToBig().String()),
		"bid-pubkey":                slotBestHeader.Capella.Capella.Message.Pubkey.String(),
	})

	go func() {
		ranking, ok := m.relayRankings.Load(*highestRankedRelay)
		if !ok {
			ranking = relayRanking{
				HeaderRequests:    0,
				HighestValueCount: 0,
			}
		}
		ranking.HighestValueCount = ranking.HighestValueCount + 1
		m.relayRankings.Store(*highestRankedRelay, ranking)
	}()

	go func() {
		if m.stats.NodeID == "" {
			return
		}

		mostProfitableBloxrouteBid, ok := m.bestBloxrouteBlockBidForSlot.Load(slot)
		if !ok {
			record := bestHeaderStatsRecord{
				Data:                   slotBestHeader.Capella.Capella,
				Slot:                   strconv.FormatUint(slot, 10),
				ProposerPublicKey:      pubkey,
				IsBloxrouteBlock:       m.isBloxrouteBlock(slotBestHeader.Capella.Capella.Message.Header.ExtraData),
				BestBloxrouteValue:     "0",
				BestBloxrouteValueDiff: big.NewInt(0).Sub(big.NewInt(0), slotBestHeader.Capella.Capella.Message.Value.ToBig()).String(),
				BestBloxrouteBlockHash: "",
				ExtraData:              common.DecodeExtraData(slotBestHeader.Capella.Capella.Message.Header.ExtraData),
				Type:                   "StatsBestHeader",
			}
			m.stats.LogToFluentD(record, time.Now(), statsNameBestHeader)
			return
		}
		bestBloxrouteValueDiff := new(big.Int).Sub(mostProfitableBloxrouteBid.Value.ToBig(), slotBestHeader.Capella.Capella.Message.Value.ToBig())

		record := bestHeaderStatsRecord{
			Data:                   slotBestHeader.Capella.Capella,
			Slot:                   strconv.FormatUint(slot, 10),
			ProposerPublicKey:      pubkey,
			IsBloxrouteBlock:       m.isBloxrouteBlock(slotBestHeader.Capella.Capella.Message.Header.ExtraData),
			BestBloxrouteValue:     mostProfitableBloxrouteBid.Value.String(),
			BestBloxrouteValueDiff: bestBloxrouteValueDiff.String(),
			BestBloxrouteBlockHash: mostProfitableBloxrouteBid.BlockHash.String(),
			ExtraData:              common.DecodeExtraData(slotBestHeader.Capella.Capella.Message.Header.ExtraData),
			Type:                   "StatsBestHeader",
		}
		m.stats.LogToFluentD(record, time.Now(), statsNameBestHeader)
	}()

	feeRecipient := slotBestHeader.Capella.Capella.Message.Header.FeeRecipient.String()
	if feeRecipient == emptyWalletAddressOne || feeRecipient == emptyWalletAddressZero {
		m.respondErrorWithLog(w, http.StatusInternalServerError, "skipping due to empty recipient wallet address", log, "")
		return
	}

	// check if request is for latest slot and if so, store the getHeader request info;
	// also store the request info if the latest slot is 0 (relay just started)
	if slot == m.latestSlotBlockReceived.Load()+1 || m.latestSlotBlockReceived.Load() == 0 {
		go func() {
			storedRequests, _ := m.getHeaderRequests.Load(slot)
			m.getHeaderRequests.Store(slot, append(storedRequests, getHeaderRequestInfo{
				requestInfo: requestInfo{
					timestamp: start,
				},
				pubkey:    pubkey,
				blockHash: slotBestHeader.Capella.Capella.Message.Header.BlockHash.String(),
			}))
		}()
	}

	responseSendTime := time.Now().UTC()

	log.WithFields(logrus.Fields{
		"requestStartTime":         start.String(),
		"responseSendTime":         responseSendTime.String(),
		"durationUntilResponseMS":  responseSendTime.Sub(start).Milliseconds(),
		"fetchGetHeaderStartTime":  fetchGetHeaderStartTime.String(),
		"fetchGetHeaderDurationMS": fetchGetHeaderDurationMS,
		"fetchGetHeaderDataSource": dataSource,
		"blockHash":                slotBestHeader.Capella.Capella.Message.Header.BlockHash.String(),
		"slotStartSec":             slotStartTimestamp,
		"msIntoSlot":               msIntoSlot,
	}).Info("returning stored header")

	success = true
	m.respondEncodedJSON(w, slotBestHeaderRaw)

	go func(getHeaderResponse *common.GetHeaderResponse) {
		providedHeaders, ok := m.providedHeaders.Load(int64(slot))
		if !ok {
			m.providedHeaders.Store(int64(slot), ProvidedHeaders{
				Headers:           []*common.GetHeaderResponse{getHeaderResponse},
				ProposerPublicKey: pubkey,
			})
		} else {
			headers := append(providedHeaders.Headers, getHeaderResponse)
			m.providedHeaders.Store(int64(slot), ProvidedHeaders{
				Headers:           headers,
				ProposerPublicKey: pubkey,
			})
		}
	}(slotBestHeader)
}

func (m *BoostService) handleGetPayload(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathGetPayload, uint64(time.Since(start).Microseconds()), success)
	}()

	log := m.log.WithFields(logrus.Fields{
		"method": "getPayload",
	})

	span := trace.SpanFromContext(req.Context())
	span.SetAttributes(
		attribute.String("method", "getPayload"),
	)

	defer req.Body.Close()

	bodyBytes := bytes.NewBuffer(nil)
	_, err := io.Copy(bodyBytes, req.Body)
	if err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, "could not process input", log.WithError(err), "")
		return
	}
	span.AddEvent("readBody")

	bodyString := bodyBytes.String()
	blockHashIndex := strings.LastIndex(bodyString, "\"block_hash\"")

	if blockHashIndex == -1 {
		m.respondErrorWithLog(w, http.StatusBadRequest, "invalid input", log, "block_hash not present")
		return
	}

	payload := new(capellaapi.SignedBlindedBeaconBlock)
	if err := decodeJSON(io.NopCloser(bodyBytes), &payload); err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log, "failed to decode request payload")
		return
	}
	span.AddEvent("decodeBody")
	slotStart := (m.genesisInfo.Data.GenesisTime + (uint64(payload.Message.Slot) * 12)) * 1000
	currTime := time.Now().UTC().UnixMilli()
	msIntoSlot := currTime - int64(slotStart)
	log.WithField("msIntoSlot", msIntoSlot).Info("getPayload request received")
	if msIntoSlot < 0 {
		// Wait until slot start (t=0) if still in the future
		_msSinceSlotStart := time.Now().UTC().UnixMilli() - int64((slotStart))
		if _msSinceSlotStart < 0 {
			delayMillis := (_msSinceSlotStart * -1) + int64(rand.Intn(50)) //nolint:gosec
			log = log.WithField("delayMillis", delayMillis)
			log.Info("waiting until slot start t=0")
			time.Sleep(time.Duration(delayMillis) * time.Millisecond)
			span.AddEvent("sleep")
		}
	} else if m.getPayloadRequestCutoffMs > 0 && msIntoSlot > int64(m.getPayloadRequestCutoffMs) {
		log.WithField("time", currTime).Error("timestamp too late")
		m.respondError(w, http.StatusBadRequest, "timestamp too late")
		span.SetStatus(codes.Error, "timestamp too late")
		return
	}

	proposerIndex := strconv.Itoa(int(payload.Message.ProposerIndex))
	proposerKey, found := m.validatorsByIndex.Load(proposerIndex)

	if !found {
		m.respondErrorWithLog(w, http.StatusBadRequest, "proposer index not found", log, "")
		return
	}

	var pub types.PublicKey
	err = pub.UnmarshalText([]byte(proposerKey))
	if err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, "invalid public key", log.WithError(err), "")
		return
	}

	// verify the signature
	ok, err := types.VerifySignature(payload.Message, m.proposerSigningDomain, pub[:], payload.Signature[:])
	if !ok || err != nil {
		if err != nil {
			log = log.WithError(err)
		}
		m.respondErrorWithLog(w, http.StatusBadRequest, "invalid signature",
			log.WithField("slot", payload.Message.Slot), "")
		return
	}

	go func() {
		m.getPayloadRequests.Store(uint64(payload.Message.Slot), requestInfo{
			timestamp: start,
		})
	}()

	blockHash := payload.Message.Body.ExecutionPayloadHeader.BlockHash

	getPayloadResponse, err := m.getGetPayloadResponse(uint64(payload.Message.Slot), blockHash.String())
	if err != nil || getPayloadResponse == nil || getPayloadResponse.Capella == nil {
		log.WithFields(logrus.Fields{
			"slot": payload.Message.Slot,
			"hash": blockHash.String(),
		}).WithError(err).Error("could not get payload from memory, redis, or db")
		span.AddEvent("could not get payload from memory, redis, or db")
	} else {
		log = log.WithFields(logrus.Fields{
			"slot":           fmt.Sprintf("%+v", payload.Message.Slot),
			"stored-payload": fmt.Sprintf("%+v", *getPayloadResponse),
			"block-hash":     blockHash.String(),
		})
		isValidatorTrusted := m.datastore.IsValidatorTrusted(context.Background(), pub.PubkeyHex())
		if isValidatorTrusted {
			m.respondOK(w, getPayloadResponse.Capella)
			span.AddEvent("responding with payload, trusted validator")
			now := time.Now().UTC().UnixMilli()
			msIntoSlot = now - int64(slotStart)
			log.WithField("msIntoSlot", msIntoSlot).Info("returning stored payload, trusted validator")
			go func() {
				if err := m.publishBlock(payload, getPayloadResponse.Capella.Capella); err != nil {
					log.WithError(err).WithFields(logrus.Fields{
						"slot":      fmt.Sprintf("%+v", payload.Message.Slot),
						"blockHash": blockHash.String(),
					}).Error("could not publish block")
				}
			}()
		} else {
			if err := m.publishBlock(payload, getPayloadResponse.Capella.Capella); err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"block-contents": *payload,
				}).Error("could not publish block")
				m.respondError(w, http.StatusBadRequest, "failed to publish block")
				return
			}

			log.Info("returning stored payload")
			m.respondOK(w, getPayloadResponse.Capella)
			span.AddEvent("responding with payload")
		}

		go func() {
			log := m.log
			blockSub, err := m.GetPayloadResponseToBlockSubmission(getPayloadResponse)
			if err != nil {
				log.WithError(err).Error("could not simulate block after responding")
				return
			}

			slotDuty, ok := m.proposerDutiesMap.Load(blockSub.Slot())
			if !ok {
				log.WithError(err).Error("could not simulate block after responding, no slot duty")
				return
			}

			simPayload := common.BuilderBlockValidationRequest{
				BuilderSubmitBlockRequest: *blockSub,
				RegisteredGasLimit:        slotDuty.GasLimit,
			}

			resChan := make(chan simulationResult)

			nodes := []string{}
			nodes = append(nodes, m.simulationNodeHighPerf)
			nodes = append(nodes, m.simulationNodes...)

			go m.simulate(context.Background(), log, &simPayload, nodes, resChan)
			// if skipSimulation is true, then res channel will be closed and won't block further execution
			simulationResult, ok := <-resChan
			if ok {
				log.Info("successful simulation for delivered payload")
			} else {
				log.WithField("simulationResult", simulationResult).Error("failed simulation for delivered payload")
			}
			close(resChan)
		}()

		// Save information about delivered payload
		go func() {
			providedHeaders, found := m.providedHeaders.Load(int64(payload.Message.Slot))
			if !found || providedHeaders.ProposerPublicKey == "" {
				log.Warnf("proposer public key not found while attempting to save delivered payload for slot %v", payload.Message.Slot)
			}
			proposerPubkey := types.PubkeyHex(providedHeaders.ProposerPublicKey)

			if m.db != nil {
				bidTrace, err := m.datastore.GetBidTrace(context.Background(), payload.Message.Body.ExecutionPayloadHeader.BlockHash.String())
				if err != nil {
					log.WithError(err).Error("failed to save delivered payload using redis, could not find bid trace")
					return
				}
				if err := m.db.SaveDeliveredPayloadFromProvidedData(uint64(payload.Message.Slot), proposerPubkey, types.Hash(payload.Message.Body.ExecutionPayloadHeader.BlockHash), payload, bidTrace, getPayloadResponse.Capella); err != nil {
					log.WithError(err).Error("failed to save delivered payload using redis")
					return
				}
			}
			if err := m.datastore.SaveDeliveredPayloadBuilderRedis(context.Background(), uint64(payload.Message.Slot), payload.Message.Body.ExecutionPayloadHeader.BlockHash.String()); err != nil {
				log.WithError(err).Error("failed to save builder delivered payload")
			}
			m.providedPayload.Store(int64(payload.Message.Slot), true)
		}()

		if m.stats.NodeID == "" {
			return
		}

		slot, proposer := uint64(0), uint64(0)
		if payload.Message != nil {
			slot = uint64(payload.Message.Slot)
			proposer = uint64(payload.Message.ProposerIndex)
		}
		log := payloadLog{
			BlockNumber:      getPayloadResponse.Capella.Capella.BlockNumber,
			Hash:             getPayloadResponse.Capella.Capella.BlockHash.String(),
			Slot:             slot,
			Proposer:         proposer,
			IsBloxrouteBlock: m.isBloxrouteBlock(getPayloadResponse.Capella.Capella.ExtraData),
			ExtraData:        common.DecodeExtraData(getPayloadResponse.Capella.Capella.ExtraData),
		}

		record := statistics.Record{
			Data: log,
			Type: "StatsBestPayload",
		}

		m.stats.LogToFluentD(record, time.Now(), statsNameBestPayload)

		return
	}

	m.respondError(w, http.StatusInternalServerError, "could not find requested payload")
}

// CheckRelays sends a request to each one of the relays previously registered to get their status
func (m *BoostService) CheckRelays() bool {
	for _, relay := range m.relays {
		code, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet, relay.GetURI(pathStatus), "", nil, nil)
		if err != nil {
			m.log.WithError(err).WithField("relay", relay).Errorf("relay check failed, response code %d", code)
			return false
		}
		m.log.WithError(err).WithField("relay", relay).Infof("relay check successful, response code %d", code)
	}

	return true
}

func (m *BoostService) handleBuilderGetValidators(w http.ResponseWriter, _ *http.Request) {
	m.proposerDutiesLock.RLock()
	defer m.proposerDutiesLock.RUnlock()
	m.respondOK(w, m.proposerDutiesResponse)
}

func (m *BoostService) handlePostBlock(w http.ResponseWriter, req *http.Request) {
	start := time.Now().UTC()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathSubmitNewBlock, uint64(time.Since(start).Microseconds()), success)
	}()

	ctx, span := m.tracer.Start(req.Context(), "handlePostBlock")
	defer span.End()
	_, initialChecks := m.tracer.Start(ctx, "initialChecks")

	var isExternalBuilder bool
	var externalBuilderAccountID string

	if checkThatRequestAuthorizedBy(req.Context(), headerAuth) {
		isExternalBuilder = true
		externalBuilderAccountID = getInfoFromRequest(req.Context(), accountIDKey).(string)
	}

	clientIPAddress := common.GetIPXForwardedFor(req)

	log := m.log.WithFields(logrus.Fields{
		"builderIP":       req.RemoteAddr,
		"clientIPAddress": clientIPAddress,
		"method":          "submitNewBlock",
	})

	authInfo, ok := req.Context().Value(authInfoKey).(authInfo)
	if !ok {
		m.respondErrorWithLog(w, http.StatusBadRequest, "could not get auth info", log, "could not decode payload")
		return
	}

	tier, ok := authInfo[accountTier]
	if !ok {
		m.respondErrorWithLog(w, http.StatusBadRequest, "could not get auth info", log, "could not decode payload")
		return
	}

	if _, ok := tier.(sdnmessage.AccountTier); !ok {
		m.respondErrorWithLog(w, http.StatusBadRequest, "could not get auth info", log, "could not decode payload")
		return
	}

	initialChecks.End()
	span.SetAttributes(
		attribute.String("builderIP", req.RemoteAddr),
		attribute.String("clientIPAddress", clientIPAddress),
		attribute.String("method", "submitNewBlock"),
	)

	_, marshalSpan := m.tracer.Start(ctx, "marshalPayload")

	payloadBytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log.WithError(err), "could not decode payload")
		return
	}
	defer req.Body.Close()

	payload := new(capellaBuilderAPI.SubmitBlockRequest)

	if req.Header.Get("Content-Type") == "application/json" {
		log = log.WithField("Content-Type", "json")
		if err := payload.UnmarshalJSON(payloadBytes); err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log.WithError(err), "could not decode JSON payload")
			marshalSpan.End()
			return
		}

	} else {
		log = log.WithField("Content-Type", "ssz")
		if err := payload.UnmarshalSSZ(payloadBytes); err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log.WithError(err), "could not decode SSZ payload")
			marshalSpan.End()
			return
		}
	}
	marshalSpan.End()

	if payload.Message == nil || payload.ExecutionPayload == nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, "skipping due to missing entries in the request", log, "invalid payload: either 'message' or 'execution_payload' are missing")
		return
	}

	var stat builderBlockReceivedStatsRecord
	_, logSpan := m.tracer.Start(ctx, "fluentdLogger")
	if m.stats.NodeID != "" {
		stat = m.createBuilderBlockReceivedStatsRecord(common.GetIPXForwardedFor(req), externalBuilderAccountID, payload, isExternalBuilder)

		defer func() {
			m.stats.LogToFluentD(
				statistics.Record{
					Data: &stat,
					Type: "NewBlockReceivedFromBuilder",
				},
				time.Now(),
				statsNameNewBlockFromBuilder)
		}()
	}
	logSpan.End()

	if err := m.handleBlockPayload(ctx, log, payload, &stat, externalBuilderAccountID, isExternalBuilder, start, clientIPAddress, req.RemoteAddr, start, tier.(sdnmessage.AccountTier)); err != nil {
		m.respondError(w, stat.HttpResponseCode, err.Error())
		return
	}

	m.respondOK(w, nilResponse)
}

func (m *BoostService) simulate(ctx context.Context, log *logrus.Entry, payload *common.BuilderBlockValidationRequest, simulationNodes []string, res chan simulationResult) {
	_, simSpan := m.tracer.Start(ctx, "blockSimulation")
	defer close(res)
	startTime := time.Now()

	log.WithField("simulation_nodes", simulationNodes).Trace("block simulation starting")

	maxTimeout := time.NewTimer(blockSimulationTimeout)
	simRes := simulationResult{}

	errChan := make(chan error, len(simulationNodes))
	passChan := make(chan fastestSimulator, len(simulationNodes))

	for _, node := range simulationNodes {
		go func(nodeURL string) {
			err := sendSim(ctx, payload, nodeURL)
			if err != nil {
				log.WithError(err).WithField("simulation_time", time.Since(startTime)).Error("block simulation failed")
				errChan <- err
				go m.datastore.SetBlockSubmissionStatus(ctx, payload.BlockHash(), datastore.BlockSimulationFailed)
				return
			} else {
				passChan <- fastestSimulator{
					nodeURL:  nodeURL,
					duration: time.Since(startTime),
				}
				go m.datastore.SetBlockSubmissionStatus(ctx, payload.BlockHash(), datastore.BlockSimulationPassed)
				return
			}
		}(node)
	}

	errorCount := 0

	for {
		select {
		case <-maxTimeout.C:
			simRes.duration = time.Since(startTime)
			simRes.errors = append(simRes.errors, errSimulationTimeout)
			log.WithError(errSimulationTimeout).Error("block simulation timeout")
			simSpan.End()
			res <- simRes
			return
		case fastest := <-passChan:
			simRes.duration = fastest.duration
			simRes.fastestNodeURL = &fastest.nodeURL
			log.WithField("simulation_time", fastest.duration).Trace("block simulation finished")
			simSpan.End()
			res <- simRes
			return
		case err := <-errChan:
			errorCount++
			simRes.errors = append(simRes.errors, err)
			if errorCount == len(simulationNodes) {
				simRes.duration = time.Since(startTime)
				simSpan.End()
				res <- simRes
				return
			}
		}
	}
}

// -----------
//  DATA APIS
// -----------

func (m *BoostService) handleDataProposerPayloadDelivered(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathDataProposerPayloadDelivered, uint64(time.Since(start).Microseconds()), success)
	}()

	var err error
	args := req.URL.Query()
	filters := database.GetPayloadsFilters{
		Limit: 100,
	}

	log := m.log.WithFields(logrus.Fields{
		"clientIPAddress": common.GetIPXForwardedFor(req),
		"method":          "getDataProposerPayloadDelivered",
	})

	if args.Get("slot") != "" && args.Get("cursor") != "" {
		m.respondErrorWithLog(w, http.StatusBadRequest, "cannot specify both slot and cursor", m.log, "")
		return
	} else if args.Get("slot") != "" {
		filters.Slot, err = strconv.ParseUint(args.Get("slot"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid slot argument", log.WithError(err), "")
			return
		}
	} else if args.Get("cursor") != "" {
		filters.Cursor, err = strconv.ParseUint(args.Get("cursor"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid cursor argument", log.WithError(err), "")
			return
		}
	}

	if args.Get("block_hash") != "" {
		var hash types.Hash
		err = hash.UnmarshalText([]byte(args.Get("block_hash")))
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid block_hash argument", log.WithError(err), "")
			return
		}
		filters.BlockHash = args.Get("block_hash")
	}

	if args.Get("block_number") != "" {
		filters.BlockNumber, err = strconv.ParseUint(args.Get("block_number"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid block_number argument", log.WithError(err), "")
			return
		}
	}

	if args.Get("builder_pubkey") != "" {
		if err = checkBLSPublicKeyHex(args.Get("builder_pubkey")); err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid builder_pubkey argument", log.WithError(err), "")
			return
		}
		filters.BuilderPubkey = args.Get("builder_pubkey")
	}

	if args.Get("limit") != "" {
		_limit, err := strconv.ParseUint(args.Get("limit"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid limit argument", log.WithError(err), "")
			return
		}
		if _limit > filters.Limit {
			m.respondErrorWithLog(w, http.StatusBadRequest, fmt.Sprintf("maximum limit is %d", filters.Limit), log, "")
			return
		}
		filters.Limit = _limit
	}

	deliveredPayloads, err := m.db.GetRecentDeliveredPayloads(filters)
	if err != nil {
		m.respondErrorWithLog(w, http.StatusInternalServerError, "error getting recent payloads", log.WithError(err), "")
		return
	}

	response := []BidTraceJSON{}
	for _, payload := range deliveredPayloads {
		trace := BidTraceJSON{
			Slot:                 payload.Slot,
			ParentHash:           payload.ParentHash,
			BlockHash:            payload.BlockHash,
			BuilderPubkey:        payload.BuilderPubkey,
			ProposerPubkey:       payload.ProposerPubkey,
			ProposerFeeRecipient: payload.ProposerFeeRecipient,
			GasLimit:             payload.GasLimit,
			GasUsed:              payload.GasUsed,
			Value:                payload.Value,
			NumTx:                uint64(payload.NumTx),
			BlockNumber:          payload.BlockNumber,
		}
		response = append(response, trace)
	}

	success = true
	m.respondOK(w, response)
}

func (m *BoostService) handleDataBuilderBidsReceived(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathDataBuilderBidsReceived, uint64(time.Since(start).Microseconds()), success)
	}()

	log := m.log.WithFields(logrus.Fields{
		"clientIPAddress": common.GetIPXForwardedFor(req),
		"method":          "getDataBuilderBidsReceived",
	})

	var err error
	args := req.URL.Query()

	filters := database.GetBuilderSubmissionsFilters{
		Limit:         500,
		Slot:          0,
		BlockHash:     "",
		BlockNumber:   0,
		BuilderPubkey: "",
	}

	if args.Get("cursor") != "" {
		m.respondErrorWithLog(w, http.StatusBadRequest, "cursor argument not supported on this API", log, "")
		return
	}

	if args.Get("slot") != "" && args.Get("cursor") != "" {
		m.respondErrorWithLog(w, http.StatusBadRequest, "cannot specify both slot and cursor", log, "")
		return
	} else if args.Get("slot") != "" {
		filters.Slot, err = strconv.ParseUint(args.Get("slot"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid slot argument", log.WithError(err), "")
			return
		}
	}

	if args.Get("builder_pubkey") != "" {
		err = checkBLSPublicKeyHex(args.Get("builder_pubkey"))
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid builder_pubkey argument", log.WithError(err), "")
			return
		}
		filters.BuilderPubkey = args.Get("builder_pubkey")
	}

	// if the current slot we are building for is requested, return empty
	currentSlot := m.latestSlotBlockReceived.Load() + 1
	if filters.Slot >= currentSlot {
		response := make([]BidTraceWithTimestampJSON, 0)
		success = true
		m.respondOK(w, response)
		return
	}

	if args.Get("block_hash") != "" {
		var hash types.Hash
		err = hash.UnmarshalText([]byte(args.Get("block_hash")))
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid block_hash argument", log.WithError(err), "")
			return
		}
		filters.BlockHash = args.Get("block_hash")
	}

	if args.Get("block_number") != "" {
		filters.BlockNumber, err = strconv.ParseUint(args.Get("block_number"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid block_number argument", log.WithError(err), "")
			return
		}
	}

	// at least one query arguments is required
	if filters.Slot == 0 && filters.BlockHash == "" && filters.BlockNumber == 0 && filters.BuilderPubkey == "" {
		m.respondErrorWithLog(w, http.StatusBadRequest, "need to query for specific slot or block_hash or block_number or builder_pubkey", log.WithError(err), "")
		return
	}

	if args.Get("limit") != "" {
		_limit, err := strconv.ParseUint(args.Get("limit"), 10, 64)
		if err != nil {
			m.respondErrorWithLog(w, http.StatusBadRequest, "invalid limit argument", log.WithError(err), "")
			return
		}
		if _limit > filters.Limit {
			m.respondErrorWithLog(w, http.StatusBadRequest, fmt.Sprintf("maximum limit is %d", filters.Limit), log, "")
			return
		}
		filters.Limit = _limit
	}

	isDefaultFilter := filters.IsDefault()

	dbResult := make(chan deliveredPayloadsResult, 1)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	go func() {
		deliveredPayloads, err := m.db.GetBuilderSubmissions(ctx, filters)
		dbResult <- deliveredPayloadsResult{deliveredPayloads, err}
	}()

	wait := true
	databaseRequestExpiration := time.After(time.Second)
	var deliveredPayloads []*database.BuilderBlockSubmissionEntry

	select {
	case <-databaseRequestExpiration:
		if isDefaultFilter {
			response, ok := m.bidTraceWithTimestampJSONCache.Get(builderBidsReceivedKey)
			if ok {
				cancel()
				success = true
				m.respondOK(w, response)
				return
			}
		}

	case result := <-dbResult:
		wait = false
		deliveredPayloads, err = result.res, result.err
	}

	if wait {
		result := <-dbResult
		deliveredPayloads, err = result.res, result.err
	}

	if err != nil {
		m.respondErrorWithLog(w, http.StatusInternalServerError, "error getting recent payloads", log.WithError(err), "")
		return
	}

	response := []BidTraceWithTimestampJSON{}
	for _, payload := range deliveredPayloads {
		// remove current slot payloads from response
		if payload.Slot == currentSlot {
			continue
		}

		trace := BidTraceWithTimestampJSON{
			Timestamp:   payload.InsertedAt.Unix(),
			TimestampMs: payload.InsertedAt.UnixMilli(),
			BidTraceJSON: BidTraceJSON{
				Slot:                 payload.Slot,
				ParentHash:           payload.ParentHash,
				BlockHash:            payload.BlockHash,
				BuilderPubkey:        payload.BuilderPubkey,
				ProposerPubkey:       payload.ProposerPubkey,
				ProposerFeeRecipient: payload.ProposerFeeRecipient,
				GasLimit:             payload.GasLimit,
				GasUsed:              payload.GasUsed,
				Value:                payload.Value,
				NumTx:                uint64(payload.NumTx),
				BlockNumber:          payload.BlockNumber,
			},
		}
		response = append(response, trace)
	}

	if isDefaultFilter {
		m.bidTraceWithTimestampJSONCache.Set(builderBidsReceivedKey, response, time.Minute*30)
	}

	success = true
	m.respondOK(w, response)
}

func (m *BoostService) handleDataValidatorRegistration(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathDataValidatorRegistration, uint64(time.Since(start).Microseconds()), success)
	}()

	log := m.log.WithFields(logrus.Fields{
		"clientIPAddress": common.GetIPXForwardedFor(req),
		"method":          "getDataValidatorRegistration",
	})

	pkStr := req.URL.Query().Get("pubkey")
	if pkStr == "" {
		m.respondErrorWithLog(w, http.StatusBadRequest, "missing pubkey argument", log, "")
		return
	}

	registration, err := m.datastore.GetValidatorRegistration(req.Context(), types.NewPubkeyHex(pkStr))
	if err != nil {
		if errors.Is(err, datastore.ErrValidatorRegistrationNotFound) {
			m.respondErrorWithLog(w, http.StatusNotFound, "no registration found for validator "+pkStr, log, "")
			return
		}
		m.respondErrorWithLog(w, http.StatusInternalServerError, "error getting validator registration", log.WithError(err), "")
		return
	}

	success = true
	m.respondOK(w, registration)
}

func (m *BoostService) updateProposerDuties(headSlot uint64) {
	// Ensure only one updating is running at a time
	if m.isUpdatingProposerDuties.Swap(true) {
		return
	}
	defer m.isUpdatingProposerDuties.Store(false)

	if headSlot%uint64(slotsPerEpoch/2) != 0 && headSlot-m.proposerDutiesSlot < uint64(slotsPerEpoch/2) {
		return
	}

	epoch := headSlot / uint64(slotsPerEpoch)

	log := m.log.WithFields(logrus.Fields{
		"epochFrom": epoch,
		"epochTo":   epoch + 1,
	})
	log.Debug("updating proposer duties...")

	// Query current epoch
	// TODO: possibly switch beacon node url to instance of 'beaconclient.IMultiBeaconClient'
	epoch1Duties, err := m.beaconClient.GetProposerDuties(epoch)
	if err != nil {
		log.WithError(err).Error("failed to get proposer duties for all beacon nodes")
		return
	}

	entries := epoch1Duties.Data

	// Query next epoch
	epoch2Duties, err := m.beaconClient.GetProposerDuties(epoch + 1)
	if epoch2Duties != nil {
		entries = append(entries, epoch2Duties.Data...)
	} else {
		log.WithError(err).Error("failed to get proposer duties for next epoch for all beacon nodes")
	}

	allProposerDuties := syncmap.NewIntegerMapOf[uint64, beaconclient.ProposerDutiesResponseData]()
	pubKeys := make([]string, len(entries))
	pubKeysSlots := make(map[string]uint64)
	for i := range entries {
		allProposerDuties.Store(entries[i].Slot, entries[i])
		pubKeys[i] = types.NewPubkeyHex(entries[i].Pubkey).String()
		pubKeysSlots[pubKeys[i]] = entries[i].Slot
	}

	// save future proposer duties including index and pubkey to send to cloud services
	m.allProposerDutiesLock.Lock()
	m.allProposerDuties = allProposerDuties
	m.allProposerDutiesLock.Unlock()

	var proposerDuties []types.BuilderGetValidatorsResponseEntry
	proposerDutiesMap := syncmap.NewIntegerMapOf[uint64, *types.RegisterValidatorRequestMessage]()

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = getValidatorRegistrationsMaxElapsedTime
	b.MaxInterval = backoff.DefaultInitialInterval

	var res []types.SignedValidatorRegistration

	err = backoff.Retry(func() error {
		var err error
		res, err = m.datastore.GetValidatorRegistrations(context.Background(), pubKeys)
		if err != nil {
			log.WithError(err).Error("failed attempt to get validator registrations")
			return err
		}
		return nil
	}, b)
	if err != nil {
		log.WithError(err).Error("failed to get validator registrations, proposer duties won't be updated")
		return
	}

	proposerDuties = make([]types.BuilderGetValidatorsResponseEntry, len(res))
	for i := range res {
		proposerDuties[i] = types.BuilderGetValidatorsResponseEntry{
			Slot:  pubKeysSlots[res[i].Message.Pubkey.PubkeyHex().String()],
			Entry: &res[i],
		}
		proposerDutiesMap.Store(pubKeysSlots[res[i].Message.Pubkey.PubkeyHex().String()], res[i].Message)
	}

	m.proposerDutiesLock.Lock()
	m.proposerDutiesResponse = proposerDuties
	m.proposerDutiesMap = proposerDutiesMap
	m.proposerDutiesSlot = headSlot
	m.proposerDutiesLock.Unlock()

	// pretty-print
	duties := make([]string, len(proposerDuties))
	for i, duty := range proposerDuties {
		duties[i] = fmt.Sprint(duty.Slot)
	}
	sort.Strings(duties)

	log.WithFields(logrus.Fields{
		"proposerDutiesCount":    len(proposerDuties),
		"allProposerDutiesCount": len(allProposerDuties.Keys()),
	}).Infof("proposer duties updated: %s", strings.Join(duties, ", "))
}

// SendCloudServicesHTTPRequest - prepare and send HTTP request, marshaling the payload if any, and decoding the response if dst is set
func (m *BoostService) SendCloudServicesHTTPRequest(ctx context.Context, client http.Client, payload any, dst any) (code int, err error) {
	var req *http.Request

	if payload == nil {
		m.log.Error("payload for cloud services HTTP request cannot be nil")
		return http.StatusNoContent, errors.New("payload for cloud services HTTP request cannot be nil")
	}

	if m.cloudServicesAuthHeader == "" {
		m.log.Error("cloud services authorization header cannot be empty")
		return http.StatusNoContent, errors.New("cloud services authorization header cannot be empty")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("could not marshal request: %w", err)
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, m.cloudServicesEndpoint, bytes.NewReader(payloadBytes))

	// Set content-type
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", m.cloudServicesAuthHeader)
	req.Header.Add("Accept", "application/json")
	if err != nil {
		return 0, fmt.Errorf("could not prepare request: %w", err)
	}

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil
	}

	if resp.StatusCode > 299 {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp.StatusCode, fmt.Errorf("could not read error response body for status code %d: %w", resp.StatusCode, err)
		}
		return resp.StatusCode, fmt.Errorf("HTTP error response: %d / %s", resp.StatusCode, string(bodyBytes))
	}

	if dst != nil {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp.StatusCode, fmt.Errorf("could not read response body: %w", err)
		}

		if err := json.Unmarshal(bodyBytes, dst); err != nil {
			return resp.StatusCode, fmt.Errorf("could not unmarshal response %s: %w", string(bodyBytes), err)
		}
	}

	return resp.StatusCode, nil
}

// handleBuilderDeleteBlocks deletes blocks from the datastore and updates the best header if needed
func (m *BoostService) handleBuilderDeleteBlocks(w http.ResponseWriter, req *http.Request) {
	log := m.log.WithFields(logrus.Fields{
		"clientIPAddress": common.GetIPXForwardedFor(req),
		"method":          "deleteBlocks",
	})

	deleteBlocksPayload := new(DeleteBidPayload)
	if err := json.NewDecoder(req.Body).Decode(deleteBlocksPayload); err != nil {
		m.respondErrorWithLog(w, http.StatusBadRequest, err.Error(), log.WithError(err), "failed to decode request payload")
		return
	}

	log.Trace("delete bids requested", "numBids", len(deleteBlocksPayload.BlockHashes))

	hashes := make(map[string]struct{})
	for _, hash := range deleteBlocksPayload.BlockHashes {
		hashes[hash] = struct{}{}
	}

	err := m.datastore.DeleteBlockSubmissions(req.Context(), deleteBlocksPayload.Slot, deleteBlocksPayload.ParentHash, deleteBlocksPayload.Pubkey, hashes)
	if err != nil {
		m.respondErrorWithLog(w, http.StatusInternalServerError, "could not delete bid", log.WithError(err), "")
		return
	}

	m.respondOK(w, nilResponse)
}

func (m *BoostService) handleActiveValidators(w http.ResponseWriter, req *http.Request) {
	activeValidatorsCount, err := m.datastore.GetActiveValidators(req.Context())
	if err != nil {
		m.respondErrorWithLog(w, http.StatusInternalServerError, "could not get active validators",
			m.log.WithField("method", "activeValidators").WithError(err), "")
		return
	}
	m.respondOK(w, map[string]int{"activeValidators": activeValidatorsCount})
}

// handleBuilderDisableGetHeaderResponse deletes blocks from the datastore and updates the best header if needed
func (m *BoostService) handleBuilderDisableGetHeaderResponse(w http.ResponseWriter, req *http.Request) {
	latestDisabledSlotUpdated := false
	if m.getHeaderLatestDisabledSlot.Load() <= m.latestSlotBlockReceived.Load() {
		m.getHeaderLatestDisabledSlot.Add(m.latestSlotBlockReceived.Load() + disableGetHeaderResponseSlotInterval)
		latestDisabledSlotUpdated = true
	}

	m.log.WithFields(logrus.Fields{
		"clientIPAddress":           common.GetIPXForwardedFor(req),
		"method":                    "postBuilderDisableGetHeaderResponse",
		"remoteAddress":             req.RemoteAddr,
		"currentSlot":               m.latestSlotBlockReceived.Load() + 1,
		"latestDisabledSlot":        m.getHeaderLatestDisabledSlot.Load(),
		"latestDisabledSlotUpdated": latestDisabledSlotUpdated,
	}).Info("BuilderDisableGetHeaderResponse called")

	m.respondOK(w, "success")
}

// StartConfigFilesLoading loads data from config files continuously based on 'configFilesUpdateInterval'
func (m *BoostService) StartConfigFilesLoading() {
	m.loadConfigFiles()
	ticker := time.NewTicker(configFilesUpdateInterval)
	for {
		<-ticker.C
		m.loadConfigFiles()
	}
}

// loadConfigFiles loads data from config files
func (m *BoostService) loadConfigFiles() {
	go m.loadHighPriorityBuilderData()
	m.loadTrustedValidatorData()
}

// isBloxrouteBlock checks block ExtraData to determine if the block was built by bloXroute
func (m *BoostService) isBloxrouteBlock(extraData types.ExtraData) bool {
	return extraData.String() == bloxrouteExtraData
}
