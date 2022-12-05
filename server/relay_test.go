package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/bloXroute-Labs/mev-relay/datastore"
	"github.com/stretchr/testify/assert"

	"github.com/alicebob/miniredis/v2"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type rpcBody struct {
	Method string `json:"method"`
}

var (
	validValidatorData = ValidatorsResponse{
		Data: []ValidatorData{
			{
				Validator: Validator{
					Pubkey: "0x98ab429cbb173ed76f2718d7ae4ab1cfe8fc36375f9d6c4618f998058d0e8b158255a4387faed53bb01cf8cb2a484a04",
				},
			},
		},
	}

	invalidValidator = Validator{
		Pubkey: "0x8c129202548f77254af7caa74c76350e1b3e33192806076cc7981b9fff54048238830219622b1f4d3e8ab175eacfd481",
	}

	header = HeaderResponse{
		Data: []headerData{
			{
				Header: headerMessage{
					Message: message{
						Slot: "1000",
					},
				},
			},
		},
	}

	testAddressHex           = "0x8dC847Af872947Ac18d5d63fA646EB65d4D99560"
	testHashHex              = "0xe28385e7bd68df656cd0042b74b69c3104b5356ed1f20eb69f1f925df47a3ab7"
	testParentHash           = _HexToHash(testHashHex)
	testInvalidPubkey        = _HexToPubkey(invalidValidator.Pubkey)
	testSlot          uint64 = 1
)

const (
	redisURI = ":6379"
	pubKey3  = "0x8a1d7b8dd64e0aafe7ea7b6c95065c9364cf99d38470c12ee807d55f7de1529ad29ce2c422e0b65e3d5a05c02caca249"
)

func saveTestBlockSubmission(t *testing.T, datastore *datastore.Datastore, parentHash types.Hash, proposerPublicKey types.PublicKey, value types.U256Str, slot uint64) {
	bidTrace := &types.SignedBidTrace{
		Signature: types.Signature{},
		Message: &types.BidTrace{
			Slot:           slot,
			ProposerPubkey: proposerPublicKey,
			Value:          value,
		},
	}

	getHeaderResponse := &types.GetHeaderResponse{
		Data: &types.SignedBuilderBid{
			Message: &types.BuilderBid{
				Header: &types.ExecutionPayloadHeader{
					ParentHash:   parentHash,
					BlockHash:    _HexToHash(testHashHex),
					FeeRecipient: _HexToAddress(testAddressHex),
				},
				Value: value,
			},
		},
	}

	getPayloadResponse := &types.GetPayloadResponse{
		Data: &types.ExecutionPayload{
			BlockHash: _HexToHash(testHashHex),
		},
	}

	err := datastore.SaveBlockSubmission(bidTrace, getHeaderResponse, getPayloadResponse)
	assert.Nil(t, err)
}

func startBeaconServer(t *testing.T) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/eth/v1/beacon/states/head/validators", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(validValidatorData); err != nil {
			require.NoError(t, err)
		}
	})
	r.HandleFunc("/eth/v1/beacon/headers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(header); err != nil {
			require.NoError(t, err)
		}
	})
	srv := &http.Server{
		Handler:      r,
		Addr:         ":8500",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	return srv
}

func startExecutionServer(t *testing.T) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		rbody := rpcBody{}

		err := json.NewDecoder(r.Body).Decode(&rbody)
		if err != nil {
			require.NoError(t, err)
		}

		switch rbody.Method {
		case "eth_blockNumber":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(blockNumberResponse{Result: "0x00001"}); err != nil {
				require.NoError(t, err)
			}

		case "eth_getBlockByNumber":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(blockResponse{
				Result: blockResult{
					TotalDifficulty: "0x64",
				},
			}); err != nil {
				require.NoError(t, err)
			}

		}

	})
	srv := &http.Server{
		Handler:      r,
		Addr:         ":8501",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	return srv
}

func startRedis(t *testing.T) {
	redisClient, err := miniredis.Run()
	assert.Nil(t, err)
	redisClient.StartAddr(redisURI)
}

func newTestBackendWithBeaconNode(t *testing.T, checkKnown bool, knownValidators string) *testBackend {
	startRedis(t)
	backend := testBackend{}
	blsPrivateKey, _, err := bls.GenerateNewKeypair()
	require.NoError(t, err)

	bn := "http://localhost:8500"

	opts := RelayServiceOpts{
		Log:                     testLog,
		ListenAddr:              "localhost:12345",
		Relays:                  []RelayEntry{newMockRelay(t, blsPrivateKey).RelayEntry},
		GenesisForkVersionHex:   "0x00000000",
		BellatrixForkVersionHex: "0x00000000",
		GenesisValidatorRootHex: "0x44f1e56283ca88b35c789f7f449e52339bc1fefe3a45913a43a6d16edcd33cf1",
		RelayRequestTimeout:     10 * time.Second,
		RelayCheck:              true,
		MaxHeaderBytes:          4000,
		IsRelay:                 false,
		SecretKey:               nil,
		PubKey:                  types.PublicKey{},
		BeaconNode:              bn,
		CheckKnownValidators:    checkKnown,
		KnownValidators:         knownValidators,
		RedisURI:                redisURI,
		DB:                      &database.MockDB{},
	}
	service, err := NewRelayService(opts)
	require.NoError(t, err)

	backend.boost = service
	return &backend
}

func newTestBackendWithBeaconAndExecutionNode(t *testing.T, ttd, mergeEpoch int) *testBackend {
	startRedis(t)
	backend := testBackend{}
	blsPrivateKey, _, err := bls.GenerateNewKeypair()
	require.NoError(t, err)

	bn := "http://localhost:8500"
	en, _ := url.Parse("http://localhost:8501")

	opts := RelayServiceOpts{
		Log:                     testLog,
		ListenAddr:              "localhost:12345",
		Relays:                  []RelayEntry{newMockRelay(t, blsPrivateKey).RelayEntry},
		GenesisForkVersionHex:   "0x00000000",
		BellatrixForkVersionHex: "0x00000000",
		GenesisValidatorRootHex: "0x44f1e56283ca88b35c789f7f449e52339bc1fefe3a45913a43a6d16edcd33cf1",
		RelayRequestTimeout:     10 * time.Second,
		RelayCheck:              true,
		MaxHeaderBytes:          4000,
		IsRelay:                 false,
		SecretKey:               nil,
		PubKey:                  types.PublicKey{},
		BeaconNode:              bn,
		ExecutionNode:           *en,
		CheckKnownValidators:    false,
		KnownValidators:         "",
		RedisURI:                redisURI,
		DB:                      &database.MockDB{},
	}
	service, err := NewRelayService(opts)
	require.NoError(t, err)

	backend.boost = service
	return &backend
}

func TestIsKnownValidator(t *testing.T) {
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	backend := newTestBackendWithBeaconNode(t, true, "")
	if err := backend.boost.FetchKnownValidators(); err != nil {
		require.NoError(t, err)
	}
	t.Run("Test isKnownValidator passes for known validator", func(t *testing.T) {
		require.Equal(t, true, backend.boost.isKnownValidator(validValidatorData.Data[0].Validator.Pubkey))
	})
	t.Run("Test isKnownValidator fails for unknown validator", func(t *testing.T) {
		require.Equal(t, false, backend.boost.isKnownValidator(invalidValidator.Pubkey))
	})
	srv.Shutdown(context.Background())
}

func TestGetHeaderWithCheckKnownValidatorsEnabled(t *testing.T) {
	getPath := func(slot uint64, parentHash types.Hash, pubkey types.PublicKey) string {
		return fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", slot, parentHash.String(), pubkey.String())
	}
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	backend := newTestBackendWithBeaconNode(t, true, "")
	if err := backend.boost.FetchKnownValidators(); err != nil {
		require.NoError(t, err)
	}

	t.Run("Test getHeader passes for known validator", func(t *testing.T) {
		pubkey := _HexToPubkey(validValidatorData.Data[0].Validator.Pubkey)
		saveTestBlockSubmission(t, backend.boost.datastore, testParentHash, pubkey, types.IntToU256(0), testSlot)
		path := getPath(1, testParentHash, pubkey)
		rr := backend.request(t, "GET", path, nil)
		require.Equal(t, 200, rr.Result().StatusCode)
	})
	t.Run("Test getHeader fails for unknown validator", func(t *testing.T) {
		path2 := getPath(1, testParentHash, testInvalidPubkey)
		rr2 := backend.request(t, "GET", path2, nil)
		fmt.Println("PathFail", path2)
		require.Equal(t, 400, rr2.Result().StatusCode)
	})
	srv.Shutdown(context.Background())
}

func TestGetHeaderWithCheckKnownValidatorsDisabled(t *testing.T) {
	getPath := func(slot uint64, parentHash types.Hash, pubkey types.PublicKey) string {
		return fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", slot, parentHash.String(), pubkey.String())
	}
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	backend := newTestBackendWithBeaconNode(t, false, "")
	if err := backend.boost.FetchKnownValidators(); err != nil {
		require.NoError(t, err)
	}
	t.Run("Test getHeader passes for unknown validator with check disabled", func(t *testing.T) {
		saveTestBlockSubmission(t, backend.boost.datastore, testParentHash, testInvalidPubkey, types.IntToU256(0), testSlot)
		path2 := getPath(testSlot, testParentHash, testInvalidPubkey)
		rr2 := backend.request(t, "GET", path2, nil)
		require.Equal(t, 200, rr2.Result().StatusCode)
	})
	srv.Shutdown(context.Background())
}

func TestRegisterWithChecksEnabled(t *testing.T) {
	path := "/eth/v1/builder/validators"
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	backend := newTestBackendWithBeaconNode(t, true, "")
	if err := backend.boost.FetchKnownValidators(); err != nil {
		require.NoError(t, err)
	}
	t.Run("Test RegisterValidator passes for known validator", func(t *testing.T) {
		reg := types.SignedValidatorRegistration{
			Message: &types.RegisterValidatorRequestMessage{
				FeeRecipient: _HexToAddress("0x0000000000000000000000000000000000000001"),
				Timestamp:    1662760198,
				GasLimit:     30000000,
				Pubkey: _HexToPubkey(
					"0x98ab429cbb173ed76f2718d7ae4ab1cfe8fc36375f9d6c4618f998058d0e8b158255a4387faed53bb01cf8cb2a484a04"),
			},
			Signature: _HexToSignature("0x87256b65f26530f1df549709fa281d3daa4c07b2e8206200745282d10ec05bc14ffcfdf542b784b7300abf09fba4dd68182eb637d26aefef7107397e36c52970fb6d6d7a2b7687da0e5cf99c32698b94a1bc89e50fe9b3d80826bd5b3d38e173"),
		}

		payload := []types.SignedValidatorRegistration{reg}
		rr := backend.request(t, http.MethodPost, path, payload)
		require.Equal(t, 200, rr.Result().StatusCode)
	})

	t.Run("Test RegisterValidator fails for unknown validator", func(t *testing.T) {
		reg := types.SignedValidatorRegistration{
			Message: &types.RegisterValidatorRequestMessage{
				FeeRecipient: _HexToAddress("0xdb65fEd33dc262Fe09D9a2Ba8F80b329BA25f941"),
				Timestamp:    1234356,
				Pubkey: _HexToPubkey(
					pubKey3),
			},
			Signature: _HexToSignature(
				"0x81510b571e22f89d1697545aac01c9ad0c1e7a3e778b3078bef524efae14990e58a6e960a152abd49de2e18d7fd3081c15d5c25867ccfad3d47beef6b39ac24b6b9fbf2cfa91c88f67aff750438a6841ec9e4a06a94ae41410c4f97b75ab284c"),
		}
		payload := []types.SignedValidatorRegistration{reg}
		rr := backend.request(t, http.MethodPost, path, payload)
		require.Equal(t, 400, rr.Result().StatusCode)
	})
	srv.Shutdown(context.Background())
}

//
// Test overridden validators
//

func TestRegisterWithChecksEnabledAndOverriden(t *testing.T) {
	path := "/eth/v1/builder/validators"
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	backend := newTestBackendWithBeaconNode(t, true, pubKey3)
	if err := backend.boost.FetchKnownValidators(); err != nil {
		require.NoError(t, err)
	}

	t.Run("Test RegisterValidator passes for unknown but overriden validator", func(t *testing.T) {
		reg := types.SignedValidatorRegistration{
			Message: &types.RegisterValidatorRequestMessage{
				FeeRecipient: _HexToAddress("0xdb65fEd33dc262Fe09D9a2Ba8F80b329BA25f941"),
				Timestamp:    1234356,
				Pubkey: _HexToPubkey(
					pubKey3),
			},
			Signature: _HexToSignature(
				"0x81510b571e22f89d1697545aac01c9ad0c1e7a3e778b3078bef524efae14990e58a6e960a152abd49de2e18d7fd3081c15d5c25867ccfad3d47beef6b39ac24b6b9fbf2cfa91c88f67aff750438a6841ec9e4a06a94ae41410c4f97b75ab284c"),
		}
		payload := []types.SignedValidatorRegistration{reg}
		rr := backend.request(t, http.MethodPost, path, payload)
		require.Equal(t, 200, rr.Result().StatusCode)
	})
	srv.Shutdown(context.Background())
}

func TestIsKnownValidatorWithOverride(t *testing.T) {
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	backend := newTestBackendWithBeaconNode(t, true, invalidValidator.Pubkey)
	if err := backend.boost.FetchKnownValidators(); err != nil {
		require.NoError(t, err)
	}
	t.Run("Test isKnownValidator passes for unknown but overridden validator", func(t *testing.T) {
		require.Equal(t, true, backend.boost.isKnownValidator(invalidValidator.Pubkey))
	})
	srv.Shutdown(context.Background())
}

func TestGetHeaderWithCheckKnownValidatorsEnabledAndOverrideSet(t *testing.T) {
	getPath := func(slot uint64, parentHash types.Hash, pubkey types.PublicKey) string {
		return fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", slot, parentHash.String(), pubkey.String())
	}
	srv := startBeaconServer(t)
	go srv.ListenAndServe()
	t.Run("Test isKnownValidator passes for unknown but overridden validator", func(t *testing.T) {
		backend := newTestBackendWithBeaconNode(t, true, invalidValidator.Pubkey)
		if err := backend.boost.FetchKnownValidators(); err != nil {
			require.NoError(t, err)
		}
		path2 := getPath(1, testParentHash, testInvalidPubkey)
		rr2 := backend.request(t, "GET", path2, nil)
		require.Equal(t, 200, rr2.Result().StatusCode)
	})
	srv.Shutdown(context.Background())
}

func TestPutRelay(t *testing.T) {
	token := AuthToken
	backend := newTestBackend(t, 1, 1, &database.MockDB{})
	t.Run("Test put relay success case", func(t *testing.T) {
		newRelay := PutRelayPayload{
			URL: "http://0xb8a0bad3f3a4f0b35418c03357c6d42017582437924a1e1ca6aee2072d5c38d321d1f8b22cd36c50b0c29187b6543b6e@test.com",
		}
		rr := backend.requestWithheader(t, "PUT", "/relays", newRelay, http.Header{"Authentication": []string{token}})
		require.Equal(t, 200, rr.Result().StatusCode)
		require.Equal(t, 2, len(backend.boost.relays))
	})
	t.Run("Test put relay when already known", func(t *testing.T) {
		newRelay := PutRelayPayload{
			URL: "http://0xb8a0bad3f3a4f0b35418c03357c6d42017582437924a1e1ca6aee2072d5c38d321d1f8b22cd36c50b0c29187b6543b6e@test.com",
		}
		rr := backend.requestWithheader(t, "PUT", "/relays", newRelay, http.Header{"Authentication": []string{token}})
		require.Equal(t, 400, rr.Result().StatusCode)
		require.Equal(t, 2, len(backend.boost.relays))
	})
	t.Run("Test put relay with no auth", func(t *testing.T) {
		newRelay := PutRelayPayload{
			URL: "http://0xb8a0bad3f3a4f0b35418c03357c6d42017582437924a1e1ca6aee2072d5c38d321d1f8b22cd36c50b0c29187b6543b6e@test.com",
		}
		rr := backend.request(t, "PUT", "/relays", newRelay)
		require.Equal(t, 401, rr.Result().StatusCode)
		require.Equal(t, 2, len(backend.boost.relays))
	})
}

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
		result := weiToEth(testCase)
		assert.Equal(t, expected, result)
	}
}
