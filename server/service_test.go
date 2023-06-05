package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/attestantio/go-builder-client/api"
	capella2 "github.com/attestantio/go-builder-client/api/capella"
	v1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-builder-client/spec"
	capellaapi "github.com/attestantio/go-eth2-client/api/v1/capella"
	"github.com/attestantio/go-eth2-client/spec/altair"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloXroute-Labs/mev-relay/beaconclient"
	"github.com/bloXroute-Labs/mev-relay/common"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/holiman/uint256"
	"github.com/redis/go-redis/v9"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/trace"

	"github.com/bloXroute-Labs/mev-relay/database"

	"github.com/flashbots/go-boost-utils/bls"

	"github.com/flashbots/go-boost-utils/types"
	"github.com/stretchr/testify/require"
)

var (
	defaultMockDB              = &database.MockDB{}
	randomPortMin              = 10000
	randomPortMax              = 20000
	testSharedRedisPort        = 18754
	testBuilderAPubkey         = common.GenerateRandomPublicKey()
	testBuilderBPubkey         = common.GenerateRandomPublicKey()
	testBuilderAPubkeyString   = testBuilderAPubkey.String()
	testBuilderBPubkeyString   = testBuilderBPubkey.String()
	testHighValueBlockHash     = common.GenerateRandomEthHash()
	testLowValueBlockHash      = common.GenerateRandomEthHash()
	testRelayAUUID, _          = uuid.NewV4()
	testHighBlockValue         = big.NewInt(100)
	testLowBlockValue          = big.NewInt(1)
	testEmptySlot              = uint64(0)
	testEmptyParentHash        = "0x0000000000000000000000000000000000000000000000000000000000000000"
	testEmptyProposerPublicKey = "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
	testRedisMessage           = &redis.Message{
		Channel:      blockSubmissionChannel,
		Pattern:      "",
		Payload:      "{\"message\":{\"signature\":\"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"message\":{\"slot\":\"0\",\"parent_hash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"block_hash\":\"0x52fdfc072182654f163f5f0f9a621d729566c74d10037c4d7bbb0407d1e2c649\",\"builder_pubkey\":\"0x81855ad8681d0d86d1e91e00167939cb6694d2c422acd208a0072939487f6999eb9d18a44784045d87f3c67cf22746e9\",\"proposer_pubkey\":\"0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"proposer_fee_recipient\":\"0x0000000000000000000000000000000000000000\",\"gas_limit\":\"0\",\"gas_used\":\"0\",\"value\":\"1\"},\"execution_payload\":{\"parent_hash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"fee_recipient\":\"0x0000000000000000000000000000000000000000\",\"state_root\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"receipts_root\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"logs_bloom\":\"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"prev_randao\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"block_number\":\"0\",\"gas_limit\":\"0\",\"gas_used\":\"0\",\"timestamp\":\"0\",\"extra_data\":\"0x\",\"base_fee_per_gas\":\"0\",\"block_hash\":\"0x0000000000000000000000000000000000000000000000000000000000000000\",\"transactions\":[],\"withdrawals\":[]}},\"sender_uuid\":\"9e0d4175-6525-4657-9118-f0109fe27990\"}",
		PayloadSlice: nil,
	}
)

type testBackend struct {
	boost  *BoostService
	relays []*mockRelay
}

// newTestBackend creates a new backend, initializes mock relays, registers them and return the instance
func newTestBackend(t *testing.T, numRelays int, relayTimeout time.Duration, db database.IDatabaseService) *testBackend {
	return newTestBackendWithRedisPort(t, numRelays, relayTimeout, db, defaultRedisPort)
}

// newTestBackendWithRedisPort creates a new backend with a random Redis port, initializes mock relays, registers them and return the instance
func newTestBackendWithRandomRedisPort(t *testing.T, numRelays int, relayTimeout time.Duration, db database.IDatabaseService) *testBackend {
	rand.Seed(time.Now().UnixNano())
	min := randomPortMin
	max := randomPortMax
	randomRedisPort := rand.Intn(max-min+1) + min

	return newTestBackendWithRedisPort(t, numRelays, relayTimeout, db, randomRedisPort)
}

// newTestBackendWithRedisPort creates a new backend with a specific Redis port, initializes mock relays, registers them and return the instance
func newTestBackendWithRedisPort(t *testing.T, numRelays int, relayTimeout time.Duration, db database.IDatabaseService, redisPort int) *testBackend {
	startRedisWithPort(t, redisPort)

	backend := testBackend{
		relays: make([]*mockRelay, numRelays),
	}

	relayEntries := make([]RelayEntry, numRelays)
	for i := 0; i < numRelays; i++ {
		// Generate private key for relay
		blsPrivateKey, _, err := bls.GenerateNewKeypair()
		require.NoError(t, err)

		// Create a mock relay
		backend.relays[i] = newMockRelay(t, blsPrivateKey)
		relayEntries[i] = backend.relays[i].RelayEntry
	}

	opts := BoostServiceOpts{
		Log:                       testLog,
		ListenAddr:                "localhost:12345",
		Relays:                    relayEntries,
		GenesisForkVersionHex:     "0x00000000",
		BellatrixForkVersionHex:   "0x00000000",
		CapellaForkVersionHex:     "0x00000000",
		GenesisValidatorRootHex:   "0x44f1e56283ca88b35c789f7f449e52339bc1fefe3a45913a43a6d16edcd33cf1",
		RelayRequestTimeout:       relayTimeout,
		RelayCheck:                true,
		MaxHeaderBytes:            4000,
		IsRelay:                   false,
		SecretKey:                 nil,
		PubKey:                    types.PublicKey{},
		CheckKnownValidators:      false,
		KnownValidators:           "",
		RedisURI:                  fmt.Sprintf("localhost:%v", redisPort),
		DB:                        db,
		GetPayloadRequestCutoffMs: 0,
	}
	service, err := NewBoostService(opts)
	require.NoError(t, err)

	beaconInstances := []beaconclient.IBeaconInstance{beaconclient.NewMockBeaconInstance()}
	service.beaconClient = *beaconclient.NewMultiBeaconClient(opts.Log, beaconInstances)
	backend.boost = service
	return &backend
}

func (be *testBackend) requestWithheader(t *testing.T, method string, path string, payload any, header http.Header) *httptest.ResponseRecorder {
	var req *http.Request
	var err error
	if payload == nil {
		req, err = http.NewRequest(method, path, bytes.NewReader(nil))
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		require.NoError(t, err2)
		req, err = http.NewRequest(method, path, bytes.NewReader(payloadBytes))
	}

	req.Header = header

	require.NoError(t, err)
	rr := httptest.NewRecorder()
	be.boost.getRouter().ServeHTTP(rr, req)
	return rr
}

func (be *testBackend) request(t *testing.T, method string, path string, payload any) *httptest.ResponseRecorder {
	var req *http.Request
	var err error

	if payload == nil {
		req, err = http.NewRequest(method, path, bytes.NewReader(nil))
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		require.NoError(t, err2)
		req, err = http.NewRequest(method, path, bytes.NewReader(payloadBytes))
	}

	require.NoError(t, err)
	rr := httptest.NewRecorder()
	be.boost.getRouter().ServeHTTP(rr, req)
	return rr
}

func TestNewBoostServiceErrors(t *testing.T) {
	t.Run("errors when no relays", func(t *testing.T) {
		_, err := NewBoostService(BoostServiceOpts{testLog, ":123", []RelayEntry{}, "0x00000000",
			"0x00000000", "0x00000000", "0x44f1e56283ca88b35c789f7f449e52339bc1fefe3a45913a43a6d16edcd33cf1",
			time.Second, true, 4000, false, nil,
			types.PublicKey{}, "", url.URL{}, url.URL{}, url.URL{},
			"", false, "", testHighPriorityBuilderPubkeys, testHighPerfSimBuilderPubkeys,
			&database.MockDB{}, "", "", "", "", "", "", "",
			"", false, RelayMaxProfit, 80, 0, []string{}, 0, 0, 0, false, trace.NewNoopTracerProvider().Tracer("test")})
		require.Error(t, err)
	})
}

func TestWebserver(t *testing.T) {
	t.Run("errors when webserver is already existing", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)
		backend.boost.srv = &http.Server{}
		err := backend.boost.StartHTTPServer()
		require.Error(t, err)
	})

	t.Run("webserver error on invalid listenAddr", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)
		backend.boost.listenAddr = "localhost:876543"
		err := backend.boost.StartHTTPServer()
		require.Error(t, err)
	})

	t.Run("webserver starts normally", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)
		go func() {
			err := backend.boost.StartHTTPServer()
			require.NoError(t, err)
		}()
		time.Sleep(time.Millisecond * 100)
		backend.boost.srv.Close()
	})
}

func TestWebserverRootHandler(t *testing.T) {
	backend := newTestBackend(t, 1, time.Second, defaultMockDB)

	// Check root handler
	req, _ := http.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	backend.boost.getRouter().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestWebserverMaxHeaderSize(t *testing.T) {
	backend := newTestBackend(t, 1, time.Second, defaultMockDB)
	addr := "localhost:1234"
	backend.boost.listenAddr = addr
	go func() {
		err := backend.boost.StartHTTPServer()
		require.NoError(t, err)
	}()
	time.Sleep(time.Millisecond * 100)
	path := "http://" + addr + "?" + strings.Repeat("abc", 4000) // path with characters of size over 4kb
	code, err := SendHTTPRequest(context.Background(), *http.DefaultClient, http.MethodGet, path, "test", nil, nil)
	require.Error(t, err)
	require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, code)
	backend.boost.srv.Close()
}

// Example good registerValidator payload
var payloadRegisterValidator = types.SignedValidatorRegistration{
	Message: &types.RegisterValidatorRequestMessage{
		FeeRecipient: _HexToAddress("0xdb65fEd33dc262Fe09D9a2Ba8F80b329BA25f941"),
		Timestamp:    1234356,
		GasLimit:     278234191203,
		Pubkey: _HexToPubkey(
			"0x8a1d7b8dd64e0aafe7ea7b6c95065c9364cf99d38470c12ee807d55f7de1529ad29ce2c422e0b65e3d5a05c02caca249"),
	},
	// Signed by 0x4e343a647c5a5c44d76c2c58b63f02cdf3a9a0ec40f102ebc26363b4b1b95033
	Signature: _HexToSignature(
		"0x8209b5391cd69f392b1f02dbc03bab61f574bb6bb54bf87b59e2a85bdc0756f7db6a71ce1b41b727a1f46ccc77b213bf0df1426177b5b29926b39956114421eaa36ec4602969f6f6370a44de44a6bce6dae2136e5fb594cce2a476354264d1ea"),
}

func TestStatus(t *testing.T) {
	t.Run("At least one relay is available", func(t *testing.T) {
		backend := newTestBackend(t, 2, time.Second, defaultMockDB)
		path := "/eth/v1/builder/status"
		rr := backend.request(t, http.MethodGet, path, payloadRegisterValidator)

		require.Equal(t, http.StatusOK, rr.Code)
		require.Equal(t, 1, backend.relays[0].GetRequestCount(path))
	})

	t.Run("No relays available", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)

		// Make the relay unavailable.
		backend.relays[0].Server.Close()

		path := "/eth/v1/builder/status"
		rr := backend.request(t, http.MethodGet, path, payloadRegisterValidator)

		require.Equal(t, http.StatusServiceUnavailable, rr.Code)
		require.Equal(t, 0, backend.relays[0].GetRequestCount(path))
	})
}

func TestRegisterValidator(t *testing.T) {
	path := "/eth/v1/builder/validators"
	reg := types.SignedValidatorRegistration{
		Message: &types.RegisterValidatorRequestMessage{
			FeeRecipient: _HexToAddress("0xdb65fEd33dc262Fe09D9a2Ba8F80b329BA25f941"),
			Timestamp:    1234356,
			Pubkey: _HexToPubkey(
				"0x8a1d7b8dd64e0aafe7ea7b6c95065c9364cf99d38470c12ee807d55f7de1529ad29ce2c422e0b65e3d5a05c02caca249"),
		},
		Signature: _HexToSignature(
			"0x81510b571e22f89d1697545aac01c9ad0c1e7a3e778b3078bef524efae14990e58a6e960a152abd49de2e18d7fd3081c15d5c25867ccfad3d47beef6b39ac24b6b9fbf2cfa91c88f67aff750438a6841ec9e4a06a94ae41410c4f97b75ab284c"),
	}
	payload := []types.SignedValidatorRegistration{reg}

	t.Run("Normal function", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)
		rr := backend.request(t, http.MethodPost, path, payload)
		time.Sleep(1 * time.Second)
		require.Equal(t, http.StatusOK, rr.Code)
	})
}

func TestGetPayload(t *testing.T) {
	path := "/eth/v1/builder/blinded_blocks"
	slot := uint64(1)
	blockHash := "0xe28385e7bd68df656cd0042b74b69c3104b5356ed1f20eb69f1f925df47a3ab1"
	blockHashTrimmed := strings.TrimPrefix(blockHash, "0x")
	parentHash := phase0.Hash32(_HexToHash("0xe28385e7bd68df656cd0042b74b69c3104b5356ed1f20eb69f1f925df47a3ab7"))

	message := &capellaapi.BlindedBeaconBlock{
		Slot:          phase0.Slot(slot),
		ProposerIndex: 1,
		ParentRoot:    phase0.Root{0x01},
		StateRoot:     phase0.Root{0x02},
		Body: &capellaapi.BlindedBeaconBlockBody{
			RANDAOReveal: phase0.BLSSignature{0xa1},
			ETH1Data: &phase0.ETH1Data{
				DepositRoot:  phase0.Root{},
				DepositCount: 0,
				BlockHash:    ethCommon.Hex2Bytes(blockHashTrimmed),
			},
			Graffiti:          [32]byte{},
			ProposerSlashings: []*phase0.ProposerSlashing{},
			AttesterSlashings: []*phase0.AttesterSlashing{},
			Attestations:      []*phase0.Attestation{},
			Deposits:          []*phase0.Deposit{},
			VoluntaryExits:    []*phase0.SignedVoluntaryExit{},
			SyncAggregate: &altair.SyncAggregate{
				SyncCommitteeBits:      ethCommon.Hex2Bytes("e28385e7bd68df656cd0042b74b69c3104b5356ed1f20eb69f1f925df47a3ab1e28385e7bd68df656cd0042b74b69c3104b5356ed1f20eb69f1f925df47a3ab1"),
				SyncCommitteeSignature: phase0.BLSSignature{},
			},
			ExecutionPayloadHeader: &capella.ExecutionPayloadHeader{
				ParentHash:      parentHash,
				BlockHash:       phase0.Hash32(_HexToHash(blockHash)),
				BlockNumber:     12345,
				FeeRecipient:    bellatrix.ExecutionAddress(_HexToAddress("0xdb65fEd33dc262Fe09D9a2Ba8F80b329BA25f941")),
				WithdrawalsRoot: phase0.Root{},
			},
			BLSToExecutionChanges: []*capella.SignedBLSToExecutionChange{},
		},
	}

	signedBidTrace := &v1.BidTrace{
		ProposerPubkey: phase0.BLSPubKey{},
		Value:          new(uint256.Int),
		Slot:           slot,
	}
	headerResp := &common.GetHeaderResponse{
		Capella: &spec.VersionedSignedBuilderBid{
			Capella: &capella2.SignedBuilderBid{
				Message: &capella2.BuilderBid{
					Header: &capella.ExecutionPayloadHeader{
						ParentHash: phase0.Hash32{},
						BlockHash:  phase0.Hash32(_HexToHash(blockHash)),
					},
					Value:  nil,
					Pubkey: phase0.BLSPubKey{},
				},
				Signature: phase0.BLSSignature{},
			},
		},
	}
	payloadResp := &api.VersionedExecutionPayload{
		Capella: &capella.ExecutionPayload{
			ParentHash:    phase0.Hash32{},
			FeeRecipient:  bellatrix.ExecutionAddress{},
			StateRoot:     [32]byte{},
			ReceiptsRoot:  [32]byte{},
			LogsBloom:     [256]byte{},
			PrevRandao:    [32]byte{},
			BlockNumber:   0,
			GasLimit:      0,
			GasUsed:       0,
			Timestamp:     0,
			ExtraData:     []byte{},
			BaseFeePerGas: [32]byte{},
			BlockHash:     phase0.Hash32(_HexToHash(blockHash)),
			Transactions:  []bellatrix.Transaction{},
			Withdrawals:   []*capella.Withdrawal{},
		},
	}

	t.Run("Okay response", func(t *testing.T) {
		backend := newTestBackendWithRandomRedisPort(t, 1, time.Second, defaultMockDB)

		sk, pk, err := bls.GenerateNewKeypair()
		require.NoError(t, err)

		root, err := types.ComputeSigningRoot(message, backend.boost.proposerSigningDomain)
		require.NoError(t, err)

		sig := bls.Sign(sk, root[:])
		sig2, err := types.SignMessage(message, backend.boost.proposerSigningDomain, sk)
		require.NoError(t, err)
		require.Equal(t, sig.Compress(), sig2[:])

		var signature types.Signature
		err = signature.FromSlice(sig.Compress())
		require.NoError(t, err)

		payload := &capellaapi.SignedBlindedBeaconBlock{
			Message:   message,
			Signature: phase0.BLSSignature(signature),
		}

		backend.boost.validatorsByIndex.Store(strconv.Itoa(int(message.ProposerIndex)), hexutil.Encode(pk.Compress()))
		err = backend.boost.datastore.SaveBlockSubmissionTx(context.Background(), nil, signedBidTrace, headerResp, payloadResp, blockHash, parentHash.String(), "")
		require.NoError(t, err)

		rr := backend.request(t, http.MethodPost, path, payload)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

		// TODO: ok to get rid of this?
		//resp := new(common.GetPayloadResponse)
		//rrBytes := rr.Body.Bytes()
		//err = json.Unmarshal(rrBytes, resp)
		//require.NoError(t, err)
	})

	t.Run("Bad response - don't store payload", func(t *testing.T) {
		backend := newTestBackendWithRandomRedisPort(t, 1, time.Second, defaultMockDB)

		sk, pk, err := bls.GenerateNewKeypair()
		require.NoError(t, err)

		root, err := types.ComputeSigningRoot(message, backend.boost.proposerSigningDomain)
		require.NoError(t, err)

		sig := bls.Sign(sk, root[:])
		sig2, err := types.SignMessage(message, backend.boost.proposerSigningDomain, sk)
		require.NoError(t, err)
		require.Equal(t, sig.Compress(), sig2[:])

		var signature types.Signature
		err = signature.FromSlice(sig.Compress())
		require.NoError(t, err)

		payload := &capellaapi.SignedBlindedBeaconBlock{
			Message:   message,
			Signature: phase0.BLSSignature(signature),
		}

		backend.boost.validatorsByIndex.Store(strconv.Itoa(int(message.ProposerIndex)), hexutil.Encode(pk.Compress()))

		rr := backend.request(t, http.MethodPost, path, payload)
		require.Equal(t, http.StatusInternalServerError, rr.Code, rr.Body.String())
	})
}

func TestCheckRelays(t *testing.T) {
	t.Run("At least one relay is okay", func(t *testing.T) {
		backend := newTestBackend(t, 3, time.Second, defaultMockDB)
		status := backend.boost.CheckRelays()
		require.Equal(t, true, status)
	})

	t.Run("Every relays are down", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)
		backend.relays[0].Server.Close()

		status := backend.boost.CheckRelays()
		require.Equal(t, false, status)
	})

	t.Run("Should not follow redirects", func(t *testing.T) {
		backend := newTestBackend(t, 1, time.Second, defaultMockDB)
		redirectAddress := backend.relays[0].Server.URL
		backend.relays[0].Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, redirectAddress, http.StatusTemporaryRedirect)
		}))

		url, err := url.ParseRequestURI(backend.relays[0].Server.URL)
		require.NoError(t, err)
		backend.boost.relays[0].URL = url
		status := backend.boost.CheckRelays()
		require.Equal(t, false, status)
	})
}

func TestHandleDataBuilderBidsReceivedReturnCorrectValueFromCache(t *testing.T) {
	db := &database.MockDB{}
	backend := newTestBackend(t, 1, time.Second, db)
	filters := database.GetBuilderSubmissionsFilters{Limit: 500}
	basePath := "/relay/v1/data/bidtraces/builder_blocks_received"
	slotQuery := "?slot="

	const (
		blockHash1             = "BlockHash"
		blockHash2             = "BlockHash2"
		blockNumber1Int uint64 = 1
		blockNumber2Int uint64 = 2
		slotNumber1Int  uint64 = 1
		slotNumber2Int  uint64 = 2
		builderPubkey1         = "BuilderPubkey"
		builderPubkey2         = "BuilderPubkey2"
	)

	slotNumber1 := fmt.Sprintf("%v", slotNumber1Int)
	slotNumber2 := fmt.Sprintf("%v", slotNumber2Int)

	// set this to the same slot as the last DB response because we do not return results from the current slot (latestSlotBlockReceived + 1)
	backend.boost.latestSlotBlockReceived.Store(2)

	dbResponse := []*database.BuilderBlockSubmissionEntry{{
		InsertedAt:           time.Now(),
		Slot:                 slotNumber1Int,
		ParentHash:           "ParentHash",
		BlockHash:            blockHash1,
		BuilderPubkey:        builderPubkey1,
		ProposerPubkey:       "ProposerPubkey",
		ProposerFeeRecipient: "ProposerFeeRecipient",
		GasUsed:              1,
		GasLimit:             1,
		Value:                "Value",
		BlockNumber:          blockNumber1Int,
	}, {
		InsertedAt:           time.Now(),
		Slot:                 slotNumber1Int,
		ParentHash:           "ParentHash2",
		BlockHash:            blockHash2,
		BuilderPubkey:        builderPubkey2,
		ProposerPubkey:       "ProposerPubkey2",
		ProposerFeeRecipient: "ProposerFeeRecipient2",
		GasUsed:              2,
		GasLimit:             2,
		Value:                "Value2",
		BlockNumber:          blockNumber2Int,
	}}

	firstExpectedResponse, err := json.Marshal([]BidTraceWithTimestampJSON{{
		BidTraceJSON: BidTraceJSON{
			Slot:                 dbResponse[0].Slot,
			ParentHash:           dbResponse[0].ParentHash,
			BlockHash:            dbResponse[0].BlockHash,
			BuilderPubkey:        dbResponse[0].BuilderPubkey,
			ProposerPubkey:       dbResponse[0].ProposerPubkey,
			ProposerFeeRecipient: dbResponse[0].ProposerFeeRecipient,
			GasLimit:             dbResponse[0].GasLimit,
			GasUsed:              dbResponse[0].GasUsed,
			Value:                dbResponse[0].Value,
			BlockNumber:          dbResponse[0].BlockNumber,
		},
		TimestampMs: dbResponse[0].InsertedAt.UnixMilli(),
		Timestamp:   dbResponse[0].InsertedAt.Unix(),
	}})
	require.NoError(t, err)

	secondExpectedResponse, err := json.Marshal([]BidTraceWithTimestampJSON{{
		BidTraceJSON: BidTraceJSON{
			Slot:                 dbResponse[1].Slot,
			ParentHash:           dbResponse[1].ParentHash,
			BlockHash:            dbResponse[1].BlockHash,
			BuilderPubkey:        dbResponse[1].BuilderPubkey,
			ProposerPubkey:       dbResponse[1].ProposerPubkey,
			ProposerFeeRecipient: dbResponse[1].ProposerFeeRecipient,
			GasLimit:             dbResponse[1].GasLimit,
			GasUsed:              dbResponse[1].GasUsed,
			Value:                dbResponse[1].Value,
			BlockNumber:          dbResponse[1].BlockNumber,
		},
		TimestampMs: dbResponse[1].InsertedAt.UnixMilli(),
		Timestamp:   dbResponse[1].InsertedAt.Unix(),
	}})
	require.NoError(t, err)

	emptyExpectedResponse, err := json.Marshal([]BidTraceWithTimestampJSON{})
	require.NoError(t, err)

	// For the first request response must be from database as firstExpectedResponse
	filters.Slot = 1
	path := basePath + slotQuery + slotNumber1
	db.Mock.On("GetBuilderSubmissions", mock.Anything, filters).
		Return([]*database.BuilderBlockSubmissionEntry{dbResponse[0]}, nil).
		After(time.Second * 2).
		Once()
	rr := backend.request(t, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, string(firstExpectedResponse), rr.Body.String())

	// For the second request we have delay 2 seconds and response must be from cache as secondExpectedResponse
	db.Mock.On("GetBuilderSubmissions", mock.Anything, filters).
		After(time.Second*2).
		Return([]*database.BuilderBlockSubmissionEntry{dbResponse[1]}, nil).
		Once()
	rr = backend.request(t, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, string(secondExpectedResponse), rr.Body.String())

	// For this third request we are requesting slot 2 so response should be an empty array
	filters.Slot = 2
	path = basePath + slotQuery + slotNumber2
	backend.boost.latestSlotBlockReceived.Store(1) // current slot is 2
	db.Mock.On("GetBuilderSubmissions", mock.Anything, filters).
		Return([]*database.BuilderBlockSubmissionEntry{dbResponse[1]}, nil)
	rr = backend.request(t, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	require.JSONEq(t, string(emptyExpectedResponse), rr.Body.String())

	// For this 4th request we are not including query parameters so response should be an error
	path = basePath
	backend.boost.latestSlotBlockReceived.Store(1) // current slot is 2
	db.Mock.On("GetBuilderSubmissions", mock.Anything, filters).
		Return([]*database.BuilderBlockSubmissionEntry{dbResponse[1]}, nil)
	rr = backend.request(t, http.MethodGet, path, nil)
	require.Equal(t, http.StatusBadRequest, rr.Code)
}
