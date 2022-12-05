package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/flashbots/go-boost-utils/types"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

const (
	weiToEthSignificantDigits = 18
)

// AuthToken used for authenticated endpoints
var AuthToken = "B71S@$742bS*$128#ldf1"

var mergeEpochSaveFile = "saved_merge_epoch.json"

// ValidatorsResponse models the response from the
// beacon node when fetching validators
type ValidatorsResponse struct {
	Data []ValidatorData `json:"data"`
}

// ValidatorData represents the data returned
// from beacon's validator endpoint
type ValidatorData struct {
	Index     string    `json:"index"`
	Balance   string    `json:"balance"`
	Status    string    `json:"status"`
	Validator Validator `json:"validator"`
}

// Validator models the validator data
type Validator struct {
	Pubkey                     string `json:"pubkey"`
	WithdrawalCredentials      string `json:"withdrawal_credentials"`
	EffectiveBalance           string `json:"effective_balance"`
	Slashed                    bool   `json:"slashed"`
	ActivationEligibilityEpoch string `json:"activation_eligibility_epoch"`
	ActivationEpoch            string `json:"activation_epoch"`
	ExitEpoch                  string `json:"exit_epoch"`
	WithdrawableEpoch          string `json:"withdrawable_epoch"`
}

// HeaderResponse models the header response
type HeaderResponse struct {
	Data []headerData `json:"data"`
}

type headerData struct {
	Header headerMessage `json:"header"`
}
type headerMessage struct {
	Message message `json:"message"`
}
type message struct {
	Slot string `json:"slot"`
}

// LogRequestID adds logging to the requests on the id parameter for the relay
func LogRequestID(logger *logrus.Entry) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)

			if id, ok := r.URL.Query()["id"]; ok && len(id) > 0 {
				logger.WithField("requesterId", id).Tracef("Request with id")
				r.AddCookie(&http.Cookie{Name: "id", Value: id[0]})
			}

		})
	}
}

// CheckKnownValidator is a middleware function that checks the endpoint and
// service config to ensure whoever is requesting the endpoint is a known
// validator
func CheckKnownValidator(m *RelayService) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			path := r.URL.Path

			if !m.checkKnownValidators || path == pathStatus || path == "/" {
				next.ServeHTTP(w, r)
				return
			}
			go m.trackActivity(path, r)
			if path == pathRegisterValidator {
				if m.checkKnownValidators {
					if ok := m.checkRegisterValidator(r); !ok {
						m.respondError(w, 400, "unknown validator")
						return
					}
				}
				next.ServeHTTP(w, r)
				return
			}
			if strings.Contains(path, "eth/v1/builder/header") {
				if m.checkKnownValidators {
					if ok := m.checkGetHeaderValidator(r); !ok {
						m.respondError(w, 400, "unknown validator")
						return
					}
				}
				next.ServeHTTP(w, r)
				return
			}

			next.ServeHTTP(w, r)

		})
	}
}

func (m *RelayService) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authentication")
		if auth != AuthToken {
			m.respondError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	}
}

// FetchKnownValidators requests the validators from the
// provided beacon node and stores them in the knownValidators variable
func (m *RelayService) FetchKnownValidators() error {
	m.log.Info("Fetching known validators")

	responsePayload := new(ValidatorsResponse)
	m.httpClient.Timeout = 2 * time.Minute
	if _, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet, fmt.Sprintf("http://%v/eth/v1/beacon/states/head/validators", m.beaconNode.Host), "mev-boost-relay", nil, responsePayload); err != nil {
		return err
	}

	for _, data := range responsePayload.Data {
		m.validators[data.Validator.Pubkey] = data.Validator
	}
	if m.knownValidators != "" {
		for _, pubkey := range strings.Split(m.knownValidators, ",") {
			m.validators[pubkey] = Validator{}
		}
	}

	m.log.Infof("Fetched %v known validators", len(m.validators))

	return nil
}

// StartFetchValidators runs a loop to fetch all validators
// every 60 minutes
func (m *RelayService) StartFetchValidators() {
	go m.FetchKnownValidators()
	ticker := time.NewTicker(60 * time.Minute)
	go func() {
		for {
			<-ticker.C
			m.FetchKnownValidators()
		}
	}()
}

func (m *RelayService) isKnownValidator(pubkey string) bool {
	_, ok := m.validators[pubkey]
	return ok
}

func (m *RelayService) checkRegisterValidator(r *http.Request) bool {
	payloads := []types.SignedValidatorRegistration{}
	if err := DecodeJSON(r.Body, &payloads); err != nil {
		m.log.Error("failed to decode validator registration", "err", err)
		return false
	}
	for _, payload := range payloads {
		if !m.isKnownValidator(payload.Message.Pubkey.String()) {
			return false
		}
	}
	data, _ := json.Marshal(payloads)
	r.Body = io.NopCloser(bytes.NewBuffer(data))
	return true
}

func (m *RelayService) checkGetHeaderValidator(r *http.Request) bool {
	vars := mux.Vars(r)
	pubkey := vars["pubkey"]
	return m.isKnownValidator(pubkey)
}

func (m *RelayService) fetchCurrentEpoch() (int, error) {

	responsePayload := new(HeaderResponse)
	m.httpClient.Timeout = 15 * time.Second
	if _, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet, fmt.Sprintf("http://%v/eth/v1/beacon/headers", m.beaconNode.Host), "mev-boost-relay", nil, responsePayload); err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(responsePayload.Data[0].Header.Message.Slot)
	if err != nil {
		return 0, err
	}

	return i / 32, nil
}

type rpcResponse struct {
	Result string `json:"result"`
}

type blockNumberResponse struct {
	Result string `json:"result"`
}

type blockResponse struct {
	Result blockResult `json:"result"`
}

type blockResult struct {
	TotalDifficulty string `json:"totalDifficulty"`
}

func (m *RelayService) getBlockNumber() (*string, error) {

	msg := []byte("{\"jsonrpc\":\"2.0\",\"method\":\"eth_blockNumber\",\"params\": [],\"id\":1}")

	blockNumRes := blockNumberResponse{}

	req, err := http.NewRequest("POST", m.executionNode.String(), bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept-Encoding", "application/json")

	res, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(res.Body).Decode(&blockNumRes); err != nil {
		return nil, err
	}

	return &blockNumRes.Result, nil
}

func (m *RelayService) fetchCurrentTTD() (*int, error) {

	blockNumber, err := m.getBlockNumber()
	if err != nil {
		return nil, err
	}

	msg := []byte(fmt.Sprintf("{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\": [\"%v\",false],\"id\":1}", *blockNumber))

	blockres := blockResponse{}

	req, err := http.NewRequest("POST", m.executionNode.String(), bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept-Encoding", "application/json")

	res, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(res.Body).Decode(&blockres); err != nil {
		return nil, err
	}

	if len(blockres.Result.TotalDifficulty) < 3 {
		return nil, errors.New("failed to get block number")
	}

	i64, err := strconv.ParseInt(blockres.Result.TotalDifficulty[2:], 16, 64)
	if err != nil {
		return nil, err
	}

	i := int(i64)
	return &i, nil
}

func loadSavedMergeEpoch() (int, error) {
	rawData, err := os.ReadFile(mergeEpochSaveFile)
	if err != nil {
		return 0, err
	}
	mergeEpoch := 0

	if err := json.Unmarshal(rawData, &mergeEpoch); err != nil {
		return 0, err
	}

	return mergeEpoch, nil
}

func saveMergeEpoch(mergeEpoch int) error {
	data, err := json.Marshal(mergeEpoch)
	if err != nil {
		return err
	}
	return os.WriteFile(mergeEpochSaveFile, data, 0644)
}

func (m *RelayService) parseRelayURLs(relayURLs string) []RelayEntry {
	ret := []RelayEntry{}
	for _, entry := range strings.Split(relayURLs, ",") {
		relay, err := NewRelayEntry(entry)
		if err != nil {
			m.log.WithError(err).WithField("relayURL", entry).Fatal("Invalid relay URL")
		}
		ret = append(ret, relay)
	}
	return ret
}

func verifyBuilderBlockSubmission(payload *types.BuilderSubmitBlockRequest) error {
	if payload.Message.BlockHash != payload.ExecutionPayload.BlockHash {
		return errors.New("blockHash mismatch")
	}

	if payload.Message.ParentHash != payload.ExecutionPayload.ParentHash {
		return errors.New("parentHash mismatch")
	}

	return nil
}

func (m *RelayService) saveBlock(slot int64, block types.ExecutionPayload, getHeaderResponse types.GetHeaderResponse, submission types.BuilderSubmitBlockRequest, signedBidTrace *types.SignedBidTrace) {
	data := types.GetPayloadResponse{
		Version: "bellatrix",
		Data:    &block,
	}

	slotBestHeader, err := m.datastore.GetGetHeaderResponse(uint64(slot), block.ParentHash.String(), signedBidTrace.Message.ProposerPubkey.String())
	isMostProfitable := slotBestHeader == nil || err != nil || slotBestHeader.Data == nil || slotBestHeader.Data.Message == nil || slotBestHeader.Data.Message.Value.Cmp(&getHeaderResponse.Data.Message.Value) < 0

	ctx, cancel := context.WithTimeout(context.Background(), databaseRequestTimeout)
	defer cancel()
	_, err = m.db.SaveBuilderBlockSubmission(ctx, &submission, nil, isMostProfitable)
	if err != nil {
		m.log.WithField("block-hash", block.BlockHash).WithError(err).Error("saving builder block submission to database failed")
	}

	if isMostProfitable {
		if err := m.datastore.SaveBlockSubmission(signedBidTrace, &getHeaderResponse, &data); err != nil {
			m.log.WithError(err).Error("failed to save the bid")
		}

		if m.cloudServicesEndpoint != "" {
			payload := blockValuePayload{
				Method: string(RPCMEVBlock),
				Params: blockValuePayloadParams{
					RelayType:            m.relayType,
					SlotNumber:           uint64(slot),
					BlockNumber:          block.BlockNumber,
					BlockHash:            block.BlockHash.String(),
					BlockValue:           getHeaderResponse.Data.Message.Value.BigInt(),
					ProposerFeeRecipient: signedBidTrace.Message.ProposerFeeRecipient.String(),
					GasUsed:              getHeaderResponse.Data.Message.Header.GasUsed,
					BuilderPubkey:        signedBidTrace.Message.BuilderPubkey.String(),
				},
			}
			go m.SendCloudServicesHTTPRequest(context.Background(), m.cloudServicesHttpClient, payload, nil)
		}
	}
}

func weiToEth(valueString string) string {
	numDigits := len(valueString)
	missing := int(math.Max(0, float64((weiToEthSignificantDigits+1)-numDigits)))
	prefix := "0000000000000000000"[:missing]
	ethValue := prefix + valueString
	decimalIndex := len(ethValue) - weiToEthSignificantDigits
	return ethValue[:decimalIndex] + "." + ethValue[decimalIndex:]
}

func SignedBlindedBeaconBlockToBeaconBlock(signedBlindedBeaconBlock *types.SignedBlindedBeaconBlock, executionPayload *types.ExecutionPayload) *types.SignedBeaconBlock {
	return &types.SignedBeaconBlock{
		Signature: signedBlindedBeaconBlock.Signature,
		Message: &types.BeaconBlock{
			Slot:          signedBlindedBeaconBlock.Message.Slot,
			ProposerIndex: signedBlindedBeaconBlock.Message.ProposerIndex,
			ParentRoot:    signedBlindedBeaconBlock.Message.ParentRoot,
			StateRoot:     signedBlindedBeaconBlock.Message.StateRoot,
			Body: &types.BeaconBlockBody{
				RandaoReveal:      signedBlindedBeaconBlock.Message.Body.RandaoReveal,
				Eth1Data:          signedBlindedBeaconBlock.Message.Body.Eth1Data,
				Graffiti:          signedBlindedBeaconBlock.Message.Body.Graffiti,
				ProposerSlashings: signedBlindedBeaconBlock.Message.Body.ProposerSlashings,
				AttesterSlashings: signedBlindedBeaconBlock.Message.Body.AttesterSlashings,
				Attestations:      signedBlindedBeaconBlock.Message.Body.Attestations,
				Deposits:          signedBlindedBeaconBlock.Message.Body.Deposits,
				VoluntaryExits:    signedBlindedBeaconBlock.Message.Body.VoluntaryExits,
				SyncAggregate:     signedBlindedBeaconBlock.Message.Body.SyncAggregate,
				ExecutionPayload:  executionPayload,
			},
		},
	}
}
