package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"

	"github.com/ethereum/go-ethereum/log"

	"github.com/bloXroute-Labs/mev-relay/beaconclient"
	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/bloXroute-Labs/mev-relay/datastore"
	"github.com/cornelk/hashmap"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	blst "github.com/supranational/blst/bindings/go"
	uberatomic "go.uber.org/atomic"
)

const (
	databaseRequestTimeout = time.Second * 12

	slotsPerEpoch                        = 32
	emptyWalletAddressZero               = "0x0000000000000000000000000000000000000000"
	emptyWalletAddressOne                = "0x0000000000000000000000000000000000000001"
	disableGetHeaderResponseSlotInterval = 10
	emptyBeaconHeadEventTimerInterval    = 3 * time.Second
	builderBidsReceivedKey               = "builderBidsReceived"
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

	errServerAlreadyRunning = errors.New("server already running")
)

var nilHash = types.Hash{}
var nilResponse = struct{}{}

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
	BlockNumber uint64 `json:"block_number"`
	Hash        string `json:"hash"`
	Slot        uint64 `json:"slot"`
	Proposer    uint64 `json:"proposer"`
}

type builderBlockReceivedStatsRecord struct {
	Slot                uint64        `json:"slot"`
	BlockHash           string        `json:"block_hash"`
	ParentHash          string        `json:"parent_hash"`
	BuilderPubkey       string        `json:"builder_pubkey"`
	ProposerPubkey      string        `json:"proposer_pubkey"`
	Value               types.U256Str `json:"value"`
	TxCount             int           `json:"tx_count"`
	AccountID           string        `json:"account_id"`
	FromInternalBuilder bool          `json:"from_internal_builder"`
}

type bestHeaderStatsRecord struct {
	Data              interface{} `json:"data"`
	Slot              string      `json:"slot"`
	ProposerPublicKey string      `json:"proposer_public_key"`
	Type              string      `json:"type"`
}

type proposerDutiesResponseData struct {
	Pubkey         string `json:"pubkey"`
	ValidatorIndex uint64 `json:"validator_index,string"`
	Slot           uint64 `json:"slot,string"`
}

type proposerDutiesResponse struct {
	Data []proposerDutiesResponseData
}

// RelayServiceOpts provides all available options for use with NewRelayService
type RelayServiceOpts struct {
	Log                     *logrus.Entry
	ListenAddr              string
	Relays                  []RelayEntry
	GenesisForkVersionHex   string
	BellatrixForkVersionHex string
	GenesisValidatorRootHex string
	RelayRequestTimeout     time.Duration
	RelayCheck              bool
	MaxHeaderBytes          int
	IsRelay                 bool
	SecretKey               *blst.SecretKey
	PubKey                  types.PublicKey
	BeaconNode              string
	ExecutionNode           url.URL
	BeaconChain             url.URL
	Etherscan               url.URL
	KnownValidators         string
	CheckKnownValidators    bool
	BuilderIPs              string
	DB                      database.IDatabaseService
	RedisURI                string
	RedisPrefix             string
	SDNURL                  string
	CertificatesPath        string
	SimulationNodes         string
	CloudServicesEndpoint   string
	CloudServicesAuthHeader string
	SendSlotProposerDuties  bool
	RelayType               RelayType
	RedisPoolSize           int
}

type ProvidedHeaders struct {
	Headers           []*types.GetHeaderResponse
	ProposerPublicKey string
}

// RelayService TODO
type RelayService struct {
	listenAddr                     string
	relays                         []RelayEntry
	log                            *logrus.Entry
	srv                            *http.Server
	relayCheck                     bool
	db                             database.IDatabaseService
	datastore                      *datastore.Datastore
	redis                          *datastore.RedisCache
	bidTraceWithTimestampJSONCache *cache.Cache

	isRelay   bool
	secretKey *blst.SecretKey
	pubKey    types.PublicKey

	beaconClient beaconclient.MultiBeaconClient
	beaconNode,
	executionNode,
	beaconChain,
	etherscan url.URL

	knownValidators         string
	checkKnownValidators    bool
	validators              map[string]Validator
	relayRankings           map[string]relayRanking
	relayRankingsLock       sync.RWMutex
	validatorActivity       *hashmap.HashMap[string, validatorActivity]
	genesisForkVersion      string
	bellatrixForkVersion    string
	proposerSigningDomain   types.Domain
	genesisValidatorRootHex string
	builderIPs              string
	certificatesPath        string
	sdnURL                  string
	simulationNodes         string

	providedHeaders *hashmap.HashMap[int64, ProvidedHeaders]
	providedPayload *hashmap.HashMap[int64, bool]

	maxHeaderBytes int

	builderSigningDomain    types.Domain
	httpClient              http.Client
	cloudServicesHttpClient http.Client
	rateLimiter             *rateLimiter

	performanceStats PerformanceStats

	proposerDutiesLock           sync.RWMutex
	proposerDutiesResponse       []types.BuilderGetValidatorsResponseEntry
	proposerDutiesSlot           uint64
	isUpdatingProposerDuties     *uberatomic.Bool
	latestFetchedSlotNumber      uint64
	cloudServicesEndpoint        string
	cloudServicesAuthHeader      string
	sendSlotProposerDuties       bool
	relayType                    RelayType
	allProposerDuties            map[uint64]beaconclient.ProposerDutiesResponseData
	allProposerDutiesLock        sync.Mutex
	latestSlotBlockReceived      uint64
	getHeaderLatestDisabledSlot  uint64
	emptyBeaconHeadEventTimer    *time.Timer
	emptyBeaconHeadEventReceived bool
}

type deliveredPayloadsResult struct {
	res []*database.BuilderBlockSubmissionEntry
	err error
}

// NewRelayService created a new RelayService
func NewRelayService(opts RelayServiceOpts) (*RelayService, error) {
	if len(opts.Relays) == 0 {
		return nil, errors.New("no relays")
	}

	builderSigningDomain, err := ComputeDomain(types.DomainTypeAppBuilder, opts.GenesisForkVersionHex, types.Root{}.String())
	if err != nil {
		return nil, err
	}

	proposerSigningDomain, err := ComputeDomain(types.DomainTypeBeaconProposer, opts.BellatrixForkVersionHex, opts.GenesisValidatorRootHex)
	if err != nil {
		return nil, err
	}

	// Connect to Redis
	redisInstance, err := datastore.NewRedisCache(opts.RedisURI, opts.RedisPrefix, opts.RedisPoolSize)
	if err != nil {
		opts.Log.WithError(err).Fatalf("Failed to connect to Redis at %s", opts.RedisURI)
	}
	opts.Log.Infof("Connected to Redis at %s", opts.RedisURI)

	datastoreInstance, err := datastore.NewDatastore(opts.Log, redisInstance, opts.DB)
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

	boostService := &RelayService{
		listenAddr:              opts.ListenAddr,
		relays:                  opts.Relays,
		log:                     opts.Log.WithField("module", "service"),
		relayCheck:              opts.RelayCheck,
		db:                      opts.DB,
		datastore:               datastoreInstance,
		redis:                   redisInstance,
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
		validators:              make(map[string]Validator),
		validatorActivity:       hashmap.New[string, validatorActivity](),
		relayRankings:           make(map[string]relayRanking),
		genesisForkVersion:      opts.GenesisForkVersionHex,
		bellatrixForkVersion:    opts.BellatrixForkVersionHex,
		proposerSigningDomain:   proposerSigningDomain,
		genesisValidatorRootHex: opts.GenesisValidatorRootHex,
		simulationNodes:         opts.SimulationNodes,
		beaconNode:              *beaconNodeURL,

		beaconClient:                   *beaconclient.NewMultiBeaconClient(opts.Log, beaconInstances),
		bidTraceWithTimestampJSONCache: cache.New(time.Minute*30, time.Minute*30),
		// TODO: possibly change providedHeaders back to hashmap.New[int64, []*types.GetHeaderResponse] after builder 'getValidators' implemented
		providedHeaders: hashmap.New[int64, ProvidedHeaders](),
		providedPayload: hashmap.New[int64, bool](),
		builderIPs:      opts.BuilderIPs,
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
		performanceStats:             NewPerformanceStats(),
		proposerDutiesLock:           sync.RWMutex{}, // sync.RWMutex
		proposerDutiesResponse:       make([]types.BuilderGetValidatorsResponseEntry, 0),
		proposerDutiesSlot:           0,                         // uint64
		isUpdatingProposerDuties:     uberatomic.NewBool(false), //uberatomic.Bool
		latestFetchedSlotNumber:      0,
		cloudServicesEndpoint:        opts.CloudServicesEndpoint,
		cloudServicesAuthHeader:      opts.CloudServicesAuthHeader,
		sendSlotProposerDuties:       opts.SendSlotProposerDuties,
		relayType:                    opts.RelayType,
		allProposerDuties:            make(map[uint64]beaconclient.ProposerDutiesResponseData),
		allProposerDutiesLock:        sync.Mutex{},
		latestSlotBlockReceived:      0,
		getHeaderLatestDisabledSlot:  0,
		rateLimiter:                  newRateLimiter(time.Second, 4),
		emptyBeaconHeadEventTimer:    nil,
		emptyBeaconHeadEventReceived: false,
	}
	boostService.emptyBeaconHeadEventTimer = time.AfterFunc(emptyBeaconHeadEventTimerInterval, func() {
		boostService.emptyBeaconHeadEventReceived = false
	})

	return boostService, nil
}

func (m *RelayService) respondError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := httpErrorResp{code, message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		m.log.WithField("response", resp).WithError(err).Error("Couldn't write error response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (m *RelayService) respondOK(w http.ResponseWriter, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		m.log.WithField("response", response).WithError(err).Error("Couldn't write OK response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (m *RelayService) getRouter() http.Handler {
	auth := NewAuth(m.log, m.certificatesPath, m.sdnURL, m.builderIPs)

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
	r.HandleFunc(pathGetHeader, m.handleGetHeader).Methods(http.MethodGet)
	r.HandleFunc(pathGetPayload, m.handleGetPayload).Methods(http.MethodPost)

	r.HandleFunc(pathSubmitNewBlock, auth.whitelistIPMiddleware(auth.authMiddleware(m.handlePostBlock))).Methods(http.MethodPost)
	r.HandleFunc(pathBuilderGetValidators, m.handleBuilderGetValidators).Methods(http.MethodGet)

	r.HandleFunc(pathDataProposerPayloadDelivered, m.handleDataProposerPayloadDelivered).Methods(http.MethodGet)
	r.HandleFunc(pathDataBuilderBidsReceived, m.handleDataBuilderBidsReceived).Methods(http.MethodGet)
	r.HandleFunc(pathDataValidatorRegistration, m.handleDataValidatorRegistration).Methods(http.MethodGet)

	r.HandleFunc(pathBuilderDeleteBlocks, auth.whitelistIPMiddleware(m.handleBuilderDeleteBlocks)).Methods(http.MethodPost)
	r.HandleFunc(pathBuilderDisableGetHeaderResponse, auth.whitelistIPMiddleware(m.handleBuilderDisableGetHeaderResponse)).Methods(http.MethodPost)

	r.Use(mux.CORSMethodMiddleware(r))
	r.Use(LogRequestID(m.log))
	r.Use(CheckKnownValidator(m))

	loggedRouter := LoggingMiddlewareLogrus(m.log, r)

	return loggedRouter
}

// StartHTTPServer starts the HTTP server for this boost service instance
func (m *RelayService) StartHTTPServer() error {
	if m.srv != nil {
		return errServerAlreadyRunning
	}

	m.srv = &http.Server{
		Addr:    m.listenAddr,
		Handler: m.getRouter(),

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
func (m *RelayService) handleRoot(w http.ResponseWriter, req *http.Request) {
	m.respondOK(w, nilResponse)
}

// handleStatus sends calls to the status endpoint of every relay.
// It returns OK if at least one returned OK, and returns error otherwise.
func (m *RelayService) handleStatus(w http.ResponseWriter, req *http.Request) {
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

	for _, r := range m.relays {
		wg.Add(1)

		go func(relay RelayEntry) {
			defer wg.Done()
			url := relay.GetURI(pathStatus)
			log := m.log.WithField("url", url)
			log.Debug("Checking relay status")

			_, err := SendHTTPRequest(ctx, m.httpClient, http.MethodGet, url, ua, nil, nil)
			if err != nil && ctx.Err() != context.Canceled {
				log.WithError(err).Error("failed to retrieve relay status")
				return
			}

			// Success: increase counter and cancel all pending requests to other relays
			atomic.AddUint32(&numSuccessRequestsToRelay, 1)
			cancel()
		}(r)
	}

	// At the end, wait for every routine and return status according to relay's ones.
	wg.Wait()

	if numSuccessRequestsToRelay > 0 {
		success = true
		m.respondOK(w, nilResponse)
	} else {
		m.respondError(w, http.StatusServiceUnavailable, "all relays are unavailable")
	}
}

// RegisterValidatorV1 - returns 200 if at least one relay returns 200
func (m *RelayService) handleRegisterValidator(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathRegisterValidator, uint64(time.Since(start).Microseconds()), success)
	}()
	log := m.log.WithField("method", "registerValidator")
	log.Info("registerValidator")

	payload := []types.SignedValidatorRegistration{}
	if err := DecodeJSON(req.Body, &payload); err != nil {
		m.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	validators := make(map[string]interface{})

	for _, registration := range payload {
		if len(registration.Signature) != 96 {
			m.respondError(w, http.StatusBadRequest, "invalid signature")
			continue
		}

		if registration.Message == nil {
			log.Debug("registration message is nil")
			continue
		}
		payloadFeeRecipient := registration.Message.FeeRecipient.String()
		payloadPublicKey := registration.Message.Pubkey.PubkeyHex()

		// Check for a previous registration fee recipient
		storedValidatorRegistration, err := m.datastore.GetValidatorRegistration(payloadPublicKey)
		if err != nil {
			log.Error("error retrieving stored validator registration", " err ", err)
		}

		// Skip signature verification if the fee recipient is the same
		if storedValidatorRegistration != nil && storedValidatorRegistration.Message.FeeRecipient.String() == payloadFeeRecipient {
			log.Tracef("validator public key %s and fee recipient %s validated from the cache", storedValidatorRegistration.Message.Pubkey.String(), payloadFeeRecipient)
			continue
		}

		ok, err := types.VerifySignature(registration.Message, m.builderSigningDomain, registration.Message.Pubkey[:], registration.Signature[:])
		if !ok || err != nil {
			log.Error("error verifying signature", " err ", err, " ok ", ok)
			m.respondError(w, http.StatusBadRequest, "invalid signature")
			continue
		} else {
			go func(reg types.SignedValidatorRegistration) {
				if err := m.datastore.SaveValidatorRegistration(registration); err != nil {
					m.log.WithError(err).Error("could not save registration for validator", "pubkey", registration.Message.Pubkey.String())
				}
			}(registration)
			validators[strings.ToLower(registration.Message.Pubkey.PubkeyHex().String())] = datastore.AliasSignedValidatorRegistration(registration)
		}
	}
	if len(validators) > 0 {
		go func() {
			err := m.datastore.SetValidatorRegistrationMap(validators)
			if err != nil {
				m.log.WithError(err).Error("Failed to set validator registration")
			} else {
				m.log.WithField("validator-count", len(validators)).Info("saved registration")
			}
		}()
	}

	log = log.WithFields(logrus.Fields{
		"numRegistrations": len(payload),
		"timeNeededSec":    time.Since(start).Seconds(),
	})
	success = true
	m.respondOK(w, nilResponse)

}

// GetHeaderV1 TODO
func (m *RelayService) handleGetHeader(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathGetHeaderPrefix, uint64(time.Since(start).Microseconds()), success)
	}()

	vars := mux.Vars(req)
	slot := vars["slot"]
	parentHashHex := vars["parent_hash"]
	pubkey := vars["pubkey"]
	log := m.log.WithFields(logrus.Fields{
		"method":     "getHeader",
		"slot":       slot,
		"parentHash": parentHashHex,
		"pubkey":     pubkey,
	})
	log.Info("getHeader")

	intSlot, err := strconv.ParseUint(slot, 10, 64)
	if err != nil {
		m.respondError(w, http.StatusBadRequest, errInvalidSlot.Error())
		return
	}

	if len(pubkey) != 98 {
		m.respondError(w, http.StatusBadRequest, errInvalidPubkey.Error())
		return
	}

	if len(parentHashHex) != 66 {
		m.respondError(w, http.StatusBadRequest, errInvalidHash.Error())
		return
	}

	if intSlot <= m.getHeaderLatestDisabledSlot {
		log.Infof("no header available, 'getHeader' requests disabled through slot %v, requested slot is %v", m.getHeaderLatestDisabledSlot, intSlot)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	highestRankedRelay := new(string)

	result := new(types.GetHeaderResponse)

	slotBestHeader, err := m.datastore.GetGetHeaderResponse(intSlot, parentHashHex, pubkey)
	if slotBestHeader == nil || err != nil {
		log.Infof("no header available in datastore for slot: %v, parent hash: %v, proposer public key: %v, error: %v", intSlot, parentHashHex, pubkey, err)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	result = slotBestHeader
	log.WithFields(logrus.Fields{
		"slot":          fmt.Sprintf("%+v", intSlot),
		"stored-header": fmt.Sprintf("%+v", *slotBestHeader.Data.Message.Header),
		"bid-header":    fmt.Sprintf("%+v", *slotBestHeader.Data.Message.Header),
		"bid-value":     fmt.Sprintf("%+v", weiToEth(slotBestHeader.Data.Message.Value.String())),
		"bid-pubkey":    fmt.Sprintf("%+v", slotBestHeader.Data.Message.Pubkey),
	}).Info("returning stored header")

	go func() {
		m.relayRankingsLock.Lock()
		defer m.relayRankingsLock.Unlock()
		ranking, ok := m.relayRankings[*highestRankedRelay]
		if !ok {
			ranking = relayRanking{
				HeaderRequests:    0,
				HighestValueCount: 0,
			}
		}
		ranking.HighestValueCount = ranking.HighestValueCount + 1
		m.relayRankings[*highestRankedRelay] = ranking
	}()
	if result.Data == nil || result.Data.Message == nil || result.Data.Message.Header == nil || result.Data.Message.Header.BlockHash == nilHash {
		log.Info("no bids received from relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	record := bestHeaderStatsRecord{
		Data:              result.Data,
		Slot:              slot,
		ProposerPublicKey: pubkey,
		Type:              "StatsBestHeader",
	}
	m.log.Info(record, time.Now(), "stats.best_header")

	if m.isRelay {
		result.Data.Message.Pubkey = m.pubKey
		sig, err := types.SignMessage(result.Data.Message, m.builderSigningDomain, m.secretKey)
		if err != nil {
			log.Fatal("could not sign message")
		}
		result.Data.Signature = sig
	}

	if result.Data.Message.Header.FeeRecipient.String() == emptyWalletAddressOne || result.Data.Message.Header.FeeRecipient.String() == emptyWalletAddressZero {
		log.Info("empty fee recipient")
		m.respondError(w, http.StatusInternalServerError, "skipping due to empty wallet address")
		return
	}

	success = true
	m.respondOK(w, result)

	go func(getHeaderResponse *types.GetHeaderResponse) {
		providedHeaders, ok := m.providedHeaders.Get(int64(intSlot))
		if !ok {
			m.providedHeaders.Set(int64(intSlot), ProvidedHeaders{
				Headers:           []*types.GetHeaderResponse{getHeaderResponse},
				ProposerPublicKey: pubkey,
			})
		} else {
			headers := append(providedHeaders.Headers, getHeaderResponse)
			m.providedHeaders.Set(int64(intSlot), ProvidedHeaders{
				Headers:           headers,
				ProposerPublicKey: pubkey,
			})
		}
	}(result)
}

func (m *RelayService) handleGetPayload(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathGetPayload, uint64(time.Since(start).Microseconds()), success)
	}()
	log := m.log.WithField("method", "getPayload")
	log.Info("getPayload")
	defer req.Body.Close()

	payload := new(types.SignedBlindedBeaconBlock)
	if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
		m.log.Warn("could not process request body after responding")
		m.respondError(w, http.StatusBadRequest, "failed to process payload request")
		return
	}

	blockHash := payload.Message.Body.ExecutionPayloadHeader.BlockHash

	ctx, cancel := context.WithTimeout(req.Context(), databaseRequestTimeout)
	defer cancel()

	getPayloadResponse, err := m.datastore.GetGetPayloadResponse(ctx, payload.Message.Slot, blockHash.String())
	if err != nil || getPayloadResponse == nil || getPayloadResponse.Data == nil {
		m.log.WithFields(logrus.Fields{
			"slot": payload.Message.Slot,
			"hash": blockHash.String(),
		}).WithError(err).Error("could not get payload from memory, redis, or db")
		m.log.Warn("block is not stored locally, fetching from relays directly")
	} else {
		log.WithFields(logrus.Fields{
			"slot":           fmt.Sprintf("%+v", payload.Message.Slot),
			"stored-payload": fmt.Sprintf("%+v", *getPayloadResponse),
		}).Info("returning stored payload")
		m.respondOK(w, getPayloadResponse)

		// Save information about delivered payload
		go func() {
			providedHeaders, found := m.providedHeaders.Get(int64(payload.Message.Slot))
			if !found || providedHeaders.ProposerPublicKey == "" {
				log.Warnf("proposer public key not found while attempting to save delivered payload for slot %v", payload.Message.Slot)
			}
			proposerPubkey := types.PubkeyHex(providedHeaders.ProposerPublicKey)

			if m.db != nil {
				err := m.db.SaveDeliveredPayload(payload.Message.Slot, proposerPubkey, blockHash, payload)
				if err != nil {
					log.WithError(err).Error("failed to save delivered payload")
				}
			}
			if err := m.publishBlock(payload, getPayloadResponse.Data); err != nil {
				m.log.WithError(err).WithField("block_hash", blockHash.String()).WithField("slot", payload.Message.Slot).WithField("block-contents", *payload).Error("could not publish block")
			}
			m.providedPayload.Set(int64(payload.Message.Slot), true)
		}()

		slot, proposer := uint64(0), uint64(0)
		if payload.Message != nil {
			slot = payload.Message.Slot
			proposer = payload.Message.ProposerIndex
		}
		log := payloadLog{
			BlockNumber: getPayloadResponse.Data.BlockNumber,
			Hash:        getPayloadResponse.Data.BlockHash.String(),
			Slot:        slot,
			Proposer:    proposer,
		}

		record := map[string]interface{}{
			"data": log,
			"type": "StatsBestPayload",
		}

		m.log.Info(record, time.Now(), "stats.best_payload")
		return
	}

	result := new(types.GetPayloadResponse)
	requestCtx, requestCtxCancel := context.WithCancel(context.Background())
	defer requestCtxCancel()
	var wg sync.WaitGroup
	var mu sync.Mutex
	ua := UserAgent(req.Header.Get("User-Agent"))

	highestRankedRelay := new(string)
	for _, relay := range m.relays {
		wg.Add(1)
		go func(relay RelayEntry) {
			defer wg.Done()
			url := relay.GetURI(pathGetPayload)
			log := log.WithField("url", url)
			log.Debug("calling getPayload")

			responsePayload := new(types.GetPayloadResponse)
			_, err := SendHTTPRequest(requestCtx, m.httpClient, http.MethodPost, url, ua, payload, responsePayload)

			if err != nil {
				log.WithError(err).Warn("error making request to relay")
				return
			}

			if responsePayload.Data == nil || responsePayload.Data.BlockHash == nilHash {
				log.Warn("invalid response")
				return
			}

			// Lock before accessing the shared payload
			mu.Lock()
			defer mu.Unlock()

			if requestCtx.Err() != nil { // request has been cancelled (or deadline exceeded)
				return
			}

			// Ensure the response blockhash matches the request
			if blockHash != responsePayload.Data.BlockHash {
				log.WithFields(logrus.Fields{
					"payloadBlockHash":  blockHash,
					"responseBlockHash": responsePayload.Data.BlockHash,
				}).Warn("requestBlockHash does not equal responseBlockHash")
				return
			}

			// Received successful response. Now cancel other requests and return immediately
			requestCtxCancel()
			*highestRankedRelay = relay.String()
			*result = *responsePayload
			log.WithFields(logrus.Fields{
				"blockHash":   responsePayload.Data.BlockHash,
				"blockNumber": responsePayload.Data.BlockNumber,
			}).Info("getPayload: received payload from relay")
		}(relay)
	}

	// Wait for all requests to complete...
	wg.Wait()

	if result.Data == nil || result.Data.BlockHash == nilHash {
		log.Warn("getPayload: no valid response from relay")
		m.respondError(w, http.StatusBadGateway, errNoSuccessfulRelayResponse.Error())
		return
	}

	// Save information about delivered payload
	go func() {
		providedHeaders, found := m.providedHeaders.Get(int64(payload.Message.Slot))
		if !found || providedHeaders.ProposerPublicKey == "" {
			log.Warnf("proposer public key not found while attempting to save delivered payload for slot %v", payload.Message.Slot)
		}
		proposerPubkey := types.PubkeyHex(providedHeaders.ProposerPublicKey)

		if m.db != nil {
			err := m.db.SaveDeliveredPayload(payload.Message.Slot, proposerPubkey, blockHash, payload)
			if err != nil {
				log.WithError(err).Error("failed to save delivered payload")
			}
		}
	}()

	logRecord := payloadLog{
		BlockNumber: result.Data.BlockNumber,
		Hash:        result.Data.BlockHash.String(),
		Slot:        payload.Message.Slot,
		Proposer:    payload.Message.ProposerIndex,
	}

	record := map[string]interface{}{
		"data": logRecord,
		"type": "StatsBestPayload",
	}

	m.log.Info(record, time.Now(), "stats.best_payload")
	success = true
	m.respondOK(w, result)

	go func() {
		if err := m.publishBlock(payload, result.Data); err != nil {
			m.log.WithError(err).WithField("block_hash", result.Data.BlockHash).WithField("slot", payload.Message.Slot).Error("could not publish block")
		}
		m.providedPayload.Set(int64(payload.Message.Slot), true)
	}()
}

// CheckRelays sends a request to each one of the relays previously registered to get their status
func (m *RelayService) CheckRelays() bool {
	for _, relay := range m.relays {
		m.log.WithField("relay", relay).Info("Checking relay")

		url := relay.GetURI(pathStatus)
		m.proposerDutiesLock.RLock()
		defer m.proposerDutiesLock.RUnlock()
		_, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet, url, "", nil, nil)
		if err != nil {
			m.log.WithError(err).WithField("relay", relay).Error("relay check failed")
			return false
		}
	}

	return true
}

func (m *RelayService) handleBuilderGetValidators(w http.ResponseWriter, req *http.Request) {
	m.proposerDutiesLock.RLock()
	defer m.proposerDutiesLock.RUnlock()
	m.respondOK(w, m.proposerDutiesResponse)
}

func (m *RelayService) handlePostBlock(w http.ResponseWriter, req *http.Request) {
	var isExternalBuilder bool
	var externalBuilderAccountID string

	if checkThatRequestAuthorizedBy(req.Context(), headerAuth) {
		isExternalBuilder = true
		externalBuilderAccountID = getInfoFromRequest(req.Context(), accountIDKey).(string)
		if !m.rateLimiter.canProcess(externalBuilderAccountID) {
			m.respondError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
	}

	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathSubmitNewBlock, uint64(time.Since(start).Microseconds()), success)
	}()
	log := m.log.WithField("method", "submitNewBlock")

	payload := new(types.BuilderSubmitBlockRequest)
	if err := json.NewDecoder(req.Body).Decode(payload); err != nil {
		log.WithError(err).Error("could not decode payload")
		m.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	if payload.Message == nil || payload.ExecutionPayload == nil {
		m.respondError(w, http.StatusBadRequest, "skipping due to missing entries in the request")
		return
	}

	if payload.Message.ProposerFeeRecipient.String() == emptyWalletAddressZero {
		m.respondError(w, http.StatusInternalServerError, "skipping due to empty fee recipient address")
		return
	}

	log = m.log.WithFields(logrus.Fields{
		"builderIP":     req.RemoteAddr,
		"builderPubKey": payload.Message.BuilderPubkey,
		"blockHash":     payload.Message.BlockHash,
		"slot":          payload.Message.Slot,
		"value":         payload.Message.Value.String(),
		"blockNumber":   payload.ExecutionPayload.BlockNumber,
	})

	wg := sync.WaitGroup{}
	wg.Add(1)
	var simError error
	var passed bool

	maxTimeout := time.NewTimer(800 * time.Millisecond)
	go func(simErrPointer *error, payloadToSimulate *types.BuilderSubmitBlockRequest, passed *bool) {
		defer wg.Done()
		log.Trace("sending block for simulation")

		errChan := make(chan error)
		passChan := make(chan bool)

		nodes := strings.Split(m.simulationNodes, ",")

		for _, node := range nodes {
			go func(nodeURL string) {
				err := sendSim(payloadToSimulate, nodeURL)
				if err != nil {
					log.WithError(err).Error("block simulation failed")
					*simErrPointer = err
					errChan <- err
					return
				} else {
					passChan <- true
					*passed = true
					return
				}
			}(node)
		}

		errorCount := 0

		for {
			select {
			case <-maxTimeout.C:
				*simErrPointer = errors.New("timeout while waiting for successful simulation response")
				log.WithError(*simErrPointer).Error("block simulation timeout")
				return
			case <-passChan:
				return
			case <-errChan:
				errorCount++
				if errorCount == len(nodes) {
					return
				}
			}
		}
	}(&simError, payload, &passed)

	log.WithField("msg", payload.Message).Info("submit block payload")

	log = log.WithFields(logrus.Fields{
		"slot":      payload.Message.Slot,
		"blockHash": payload.Message.BlockHash.String(),
	})

	// Sanity check the submission
	err := verifyBuilderBlockSubmission(payload)
	if err != nil {
		log.WithError(err).Warn("block submission sanity checks failed")
		m.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Verify the signature
	ok, err := types.VerifySignature(payload.Message, m.builderSigningDomain, payload.Message.BuilderPubkey[:], payload.Signature[:])
	if !ok || err != nil {
		log.WithError(err).Warnf("could not verify builder signature")
		m.respondError(w, http.StatusBadRequest, "invalid signature")
		return
	}

	header, err := types.PayloadToPayloadHeader(payload.ExecutionPayload)
	if err != nil {
		m.log.Fatal(err)
	}

	builderBid := types.BuilderBid{
		Value:  payload.Message.Value,
		Header: header,
		Pubkey: m.pubKey,
	}

	sig, err := types.SignMessage(&builderBid, m.builderSigningDomain, m.secretKey)
	if err != nil {
		m.log.Fatal("could not sign message")
	}
	signedBuilderBid := types.SignedBuilderBid{
		Message:   &builderBid,
		Signature: sig,
	}
	signedBidTrace := types.SignedBidTrace{
		Message:   payload.Message,
		Signature: sig,
	}

	getHeaderResponse := types.GetHeaderResponse{
		Version: "bellatrix",
		Data:    &signedBuilderBid,
	}

	wg.Wait()

	if !passed {
		m.log.WithError(simError).Error("block failed simulation with error")
		m.respondError(w, 400, fmt.Sprintf("block failed simulation with error: %v", simError))
		return
	}

	go func() {
		m.saveBlock(int64(payload.Message.Slot), *payload.ExecutionPayload, getHeaderResponse, *payload, &signedBidTrace)
	}()

	logFields := logrus.Fields{
		"slot":           payload.Message.Slot,
		"blockHash":      payload.Message.BlockHash.String(),
		"parentHash":     payload.Message.ParentHash.String(),
		"proposerPubkey": payload.Message.ProposerPubkey.String(),
		"value":          weiToEth(payload.Message.Value.String()),
		"tx":             len(payload.ExecutionPayload.Transactions),
	}

	stat := builderBlockReceivedStatsRecord{
		Slot:                payload.Message.Slot,
		BlockHash:           payload.Message.BlockHash.String(),
		ParentHash:          payload.Message.ParentHash.String(),
		BuilderPubkey:       payload.Message.BuilderPubkey.String(),
		ProposerPubkey:      payload.Message.ProposerPubkey.String(),
		Value:               payload.Message.Value,
		TxCount:             len(payload.ExecutionPayload.Transactions),
		AccountID:           "",
		FromInternalBuilder: true,
	}

	if isExternalBuilder {
		logFields["accountID"] = externalBuilderAccountID
		stat.AccountID = fmt.Sprintf("%s", externalBuilderAccountID)
		stat.FromInternalBuilder = false
	}

	log.WithFields(logFields).Info("received new block from builder")

	record := map[string]interface{}{
		"data": &stat,
		"type": "NewBlockReceivedFromBuilder",
	}
	m.log.Info(record, time.Now(), "stats.new_block_from_builder")

	// Respond with OK (TODO: proper response format)
	success = true
	w.WriteHeader(http.StatusOK)

	go func() {
		if _, ok := m.providedHeaders.Get(int64(payload.Message.Slot)); !ok {
			m.providedHeaders.Set(int64(payload.Message.Slot), ProvidedHeaders{
				Headers:           make([]*types.GetHeaderResponse, 0),
				ProposerPublicKey: payload.Message.ProposerPubkey.String(),
			})
		}
	}()
}

// -----------
//  DATA APIS
// -----------

func (m *RelayService) handleDataProposerPayloadDelivered(w http.ResponseWriter, req *http.Request) {
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

	if args.Get("slot") != "" && args.Get("cursor") != "" {
		m.respondError(w, http.StatusBadRequest, "cannot specify both slot and cursor")
		return
	} else if args.Get("slot") != "" {
		filters.Slot, err = strconv.ParseUint(args.Get("slot"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid slot argument")
			return
		}
	} else if args.Get("cursor") != "" {
		filters.Cursor, err = strconv.ParseUint(args.Get("cursor"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid cursor argument")
			return
		}
	}

	if args.Get("block_hash") != "" {
		var hash types.Hash
		err = hash.UnmarshalText([]byte(args.Get("block_hash")))
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid block_hash argument")
			return
		}
		filters.BlockHash = args.Get("block_hash")
	}

	if args.Get("block_number") != "" {
		filters.BlockNumber, err = strconv.ParseUint(args.Get("block_number"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid block_number argument")
			return
		}
	}

	if args.Get("limit") != "" {
		_limit, err := strconv.ParseUint(args.Get("limit"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid limit argument")
			return
		}
		if _limit > filters.Limit {
			m.respondError(w, http.StatusBadRequest, fmt.Sprintf("maximum limit is %d", filters.Limit))
			return
		}
		filters.Limit = _limit
	}

	deliveredPayloads, err := m.db.GetRecentDeliveredPayloads(filters)
	if err != nil {
		m.log.WithError(err).Error("error getting recent payloads")
		m.respondError(w, http.StatusInternalServerError, err.Error())
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

func (m *RelayService) handleDataBuilderBidsReceived(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathDataBuilderBidsReceived, uint64(time.Since(start).Microseconds()), success)
	}()

	var err error
	args := req.URL.Query()

	filters := database.GetBuilderSubmissionsFilters{
		Limit:       100,
		Slot:        0,
		BlockHash:   "",
		BlockNumber: 0,
	}

	if args.Get("cursor") != "" {
		m.respondError(w, http.StatusBadRequest, "cursor argument not supported on this API")
		return
	}

	if args.Get("slot") != "" && args.Get("cursor") != "" {
		m.respondError(w, http.StatusBadRequest, "cannot specify both slot and cursor")
		return
	} else if args.Get("slot") != "" {
		filters.Slot, err = strconv.ParseUint(args.Get("slot"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid slot argument")
			return
		}
	}

	if args.Get("block_hash") != "" {
		var hash types.Hash
		err = hash.UnmarshalText([]byte(args.Get("block_hash")))
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid block_hash argument")
			return
		}
		filters.BlockHash = args.Get("block_hash")
	}

	if args.Get("block_number") != "" {
		filters.BlockNumber, err = strconv.ParseUint(args.Get("block_number"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid block_number argument")
			return
		}
	}

	if args.Get("limit") != "" {
		_limit, err := strconv.ParseUint(args.Get("limit"), 10, 64)
		if err != nil {
			m.respondError(w, http.StatusBadRequest, "invalid limit argument")
			return
		}
		if _limit > filters.Limit {
			m.respondError(w, http.StatusBadRequest, fmt.Sprintf("maximum limit is %d", filters.Limit))
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
		m.log.WithError(err).Error("error getting recent payloads")
		m.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := []BidTraceWithTimestampJSON{}
	for _, payload := range deliveredPayloads {
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
				BlockNumber:          uint64(payload.BlockNumber),
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

func (m *RelayService) handleDataValidatorRegistration(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	success := false
	defer func() {
		m.performanceStats.SetEndpointStats(pathDataValidatorRegistration, uint64(time.Since(start).Microseconds()), success)
	}()

	pkStr := req.URL.Query().Get("pubkey")
	if pkStr == "" {
		m.respondError(w, http.StatusBadRequest, "missing pubkey argument")
		return
	}

	registration, err := m.redis.GetValidatorRegistration(types.NewPubkeyHex(pkStr))
	if err != nil {
		m.log.WithError(err).Error("error getting validator registration")
		m.respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if registration == nil {
		m.respondError(w, http.StatusBadRequest, "no registration found for validator "+pkStr)
		return
	}

	success = true
	m.respondOK(w, registration)
}

func (m *RelayService) updateProposerDuties(headSlot uint64) {
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

	// Validator registrations are queried in parallel, and this is the result struct
	type result struct {
		val types.BuilderGetValidatorsResponseEntry
		err error
	}

	// Scatter requests to Redis to get validator registrations
	c := make(chan result, len(entries))
	allProposerDuties := make(map[uint64]beaconclient.ProposerDutiesResponseData)
	for i := 0; i < cap(c); i++ {
		go func(duty beaconclient.ProposerDutiesResponseData) {
			reg, err := m.redis.GetValidatorRegistration(types.NewPubkeyHex(duty.Pubkey))
			c <- result{types.BuilderGetValidatorsResponseEntry{
				Slot:  duty.Slot,
				Entry: reg,
			}, err}
		}(entries[i])

		allProposerDuties[entries[i].Slot] = entries[i]
	}

	// save future proposer duties including index and pubkey to send to cloud services
	m.allProposerDutiesLock.Lock()
	m.allProposerDuties = allProposerDuties
	m.allProposerDutiesLock.Unlock()

	// Gather results
	proposerDuties := make([]types.BuilderGetValidatorsResponseEntry, 0)
	for i := 0; i < cap(c); i++ {
		res := <-c
		if res.err != nil {
			log.WithError(res.err).Error("error in loading validator registration from redis")
		} else if res.val.Entry != nil { // only if a known registration
			proposerDuties = append(proposerDuties, res.val)
		}
	}

	if err == nil {
		m.proposerDutiesLock.Lock()
		m.proposerDutiesResponse = proposerDuties
		m.proposerDutiesSlot = headSlot
		m.proposerDutiesLock.Unlock()

		// pretty-print
		_duties := make([]string, len(proposerDuties))
		for i, duty := range proposerDuties {
			_duties[i] = fmt.Sprint(duty.Slot)
		}
		sort.Strings(_duties)
		m.log.Infof("proposer duties updated: %s", strings.Join(_duties, ", "))
	} else {
		m.log.WithError(err).Error("failed to update proposer duties")
	}
}

// GetProposerDuties returns proposer duties for every slot in this epoch
// https://ethereum.github.io/beacon-APIs/#/Validator/getProposerDuties
func (m *RelayService) GetProposerDuties(epoch uint64) (*proposerDutiesResponse, error) {
	uri := fmt.Sprintf("http://%v/eth/v1/validator/duties/proposer/%d", m.beaconNode.Host, epoch)
	resp := &proposerDutiesResponse{}
	_, err := fetchBeacon(http.MethodGet, uri, nil, resp)
	return resp, err
}

// SendCloudServicesHTTPRequest - prepare and send HTTP request, marshaling the payload if any, and decoding the response if dst is set
func (m *RelayService) SendCloudServicesHTTPRequest(ctx context.Context, client http.Client, payload any, dst any) (code int, err error) {
	var req *http.Request

	if payload == nil {
		log.Error("payload for cloud services HTTP request cannot be nil")
		return http.StatusNoContent, errors.New("payload for cloud services HTTP request cannot be nil")
	}

	if m.cloudServicesAuthHeader == "" {
		log.Error("cloud services authorization header cannot be empty")
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
func (m *RelayService) handleBuilderDeleteBlocks(w http.ResponseWriter, req *http.Request) {
	log := m.log.WithField("method", "deleteBlocks")
	deleteBlocksPayload := new(DeleteBidPayload)

	if err := json.NewDecoder(req.Body).Decode(deleteBlocksPayload); err != nil {
		log.WithError(err).Error("could not decode payload")
		m.respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	log.Info("delete bids requested", "numBids", len(deleteBlocksPayload.BlockHashes))

	hashes := map[string]bool{}
	for _, hash := range deleteBlocksPayload.BlockHashes {
		hashes[hash] = true
	}

	if err := m.datastore.DeleteBlockSubmissions(deleteBlocksPayload.Slot, deleteBlocksPayload.ParentHash, deleteBlocksPayload.Pubkey, hashes); err != nil {
		m.respondError(w, http.StatusInternalServerError, "could not delete bid")
		return
	}

	m.respondOK(w, nilResponse)
}

// handleBuilderDisableGetHeaderResponse deletes blocks from the datastore and updates the best header if needed
func (m *RelayService) handleBuilderDisableGetHeaderResponse(w http.ResponseWriter, req *http.Request) {
	latestDisabledSlotUpdated := false
	if m.getHeaderLatestDisabledSlot <= m.latestSlotBlockReceived {
		m.getHeaderLatestDisabledSlot = m.latestSlotBlockReceived + disableGetHeaderResponseSlotInterval
		latestDisabledSlotUpdated = true
	}

	m.log.WithFields(logrus.Fields{
		"remoteAddress":             req.RemoteAddr,
		"currentSlot":               m.latestSlotBlockReceived + 1,
		"latestDisabledSlot":        m.getHeaderLatestDisabledSlot,
		"latestDisabledSlotUpdated": latestDisabledSlotUpdated,
	}).Info("BuilderDisableGetHeaderResponse called")

	m.respondOK(w, "success")
}
