package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"

	"github.com/puzpuzpuz/xsync/v2"

	capellaapi2 "github.com/attestantio/go-builder-client/api/capella"
	capellaapi "github.com/attestantio/go-eth2-client/api/v1/capella"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	utilcapella "github.com/attestantio/go-eth2-client/util/capella"
	"github.com/bloXroute-Labs/gateway/v2/logger"
	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/services/statistics"
	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/cenkalti/backoff/v4"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	v1 "github.com/attestantio/go-builder-client/api/v1"
)

const (
	saveBlockToDatabaseMaxElapsedTime = 20 * time.Second
	saveBlockToDatabaseMaxInterval    = 5 * time.Second
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
func LogRequestID(m *BoostService) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if id, ok := r.URL.Query()["id"]; ok && len(id) > 0 {
				m.log.WithField("requesterId", id).Tracef("Request with id")
				r.AddCookie(&http.Cookie{Name: "id", Value: id[0]})
			}

			next.ServeHTTP(w, r)

		})
	}
}

// CheckKnownValidator is a middleware function that checks the endpoint and
// service config to ensure whoever is requesting the endpoint is a known
// validator
func CheckKnownValidator(m *BoostService) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			path := r.URL.Path

			if !m.checkKnownValidators || path == pathStatus || path == "/" {
				next.ServeHTTP(w, r)
				return
			}

			go m.trackActivity(path, r)

			if m.checkKnownValidators {
				ok := true
				if path == pathRegisterValidator {
					ok = m.checkRegisterValidator(r)
				}
				if strings.Contains(path, "eth/v1/builder/header") {
					ok = m.checkGetHeaderValidator(r)
				}
				if !ok {
					m.respondErrorWithLog(w, http.StatusBadRequest, "unknown validator",
						m.log, "")
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}

func (m *BoostService) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authentication")
		if auth != AuthToken {
			m.respondErrorWithLog(w, http.StatusUnauthorized, "unauthorized",
				m.log.WithField("clientIPAddress", common.GetIPXForwardedFor(r)), "")
			return
		}
		next.ServeHTTP(w, r)
	}
}

// NewLog builds new log configuration from the CLI context
func NewLog(consoleLevelFlag, fileLevelFlag string, fileMaxSize, fileMaxAge, fileMaxBackups int) (*logger.Config, error) {
	appName := "mev-boost-relay"
	consoleLevel, err := logger.ParseLevel(consoleLevelFlag)
	if err != nil {
		return nil, err
	}

	fileLevel, err := logger.ParseLevel(fileLevelFlag)
	if err != nil {
		return nil, err
	}

	logConfig := logger.Config{
		AppName:      appName,
		FileName:     fmt.Sprintf("../../logs/%v.log", appName),
		FileLevel:    fileLevel,
		ConsoleLevel: consoleLevel,
		MaxSize:      fileMaxSize,
		MaxBackups:   fileMaxBackups,
		MaxAge:       fileMaxAge,
	}

	return &logConfig, nil
}

// InitLogger initializes a new bloxroute logger using logrus
func InitLogger(consoleLevelFlag, fileLevelFlag, fluentdHost, nodeID string, fluentdEnabled bool, fileMaxSize, fileMaxAge, fileMaxBackups int) {
	config, err := NewLog(consoleLevelFlag, fileLevelFlag, fileMaxSize, fileMaxAge, fileMaxBackups)
	if err != nil {
		logger.Fatal(err)
		return
	}
	if err := logger.Init(config, ""); err != nil {
		logger.Fatal(err)
		return
	}

	if err = logger.InitFluentD(fluentdEnabled, fluentdHost, nodeID, logrus.InfoLevel); err != nil {
		logger.Fatal(err)
		return
	}

}

// FetchKnownValidators requests the validators from the
// provided beacon node and stores them in the knownValidators variable
func (m *BoostService) FetchKnownValidators() error {
	m.log.Info("Fetching known validators")

	responsePayload := new(ValidatorsResponse)
	m.httpClient.Timeout = 2 * time.Minute
	if _, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet,
		fmt.Sprintf("http://%v/eth/v1/beacon/states/head/validators?status=active,pending", m.beaconNode.Host),
		"mev-boost-relay", nil, responsePayload); err != nil {
		return err
	}

	for _, data := range responsePayload.Data {
		m.validators.Store(data.Validator.Pubkey, data.Validator)
		m.validatorsByIndex.Store(data.Index, data.Validator.Pubkey)
	}
	if m.knownValidators != "" {
		var i int
		for _, pubkey := range strings.Split(m.knownValidators, ",") {
			// since the validator indexes are sequential, a negative value is being used
			// to avoid any collisions
			i--
			m.validatorsByIndex.Store(strconv.Itoa(i), pubkey)
			m.validators.Store(pubkey, Validator{})
		}
	}

	// not sure if we need lock to check length?
	m.validatorsLock.RLock()
	m.log.Infof("Fetched %v known validators", len(m.validators.Keys()))
	m.validatorsLock.RUnlock()

	return nil
}

// StartFetchValidators runs a loop to fetch all validators
// every 60 minutes
func (m *BoostService) StartFetchValidators() {
	go m.FetchKnownValidators()
	ticker := time.NewTicker(60 * time.Minute)
	go func() {
		for {
			<-ticker.C
			m.FetchKnownValidators()
		}
	}()
}

func (m *BoostService) UpdateActiveValidators() {
	if err := m.datastore.UpdateActiveValidators(); err != nil {
		m.log.WithError(err).Error("could not update active validator count")
	}
	ticker := time.NewTicker(60 * time.Minute)
	go func() {
		for {
			<-ticker.C
			secondsIntoSlot := (uint64(time.Now().UTC().Unix()) - m.genesisInfo.Data.GenesisTime) % 12
			secondsPastSix := 5 - int64(secondsIntoSlot)
			if secondsPastSix < 0 {
				time.Sleep(time.Duration(12+secondsPastSix) * time.Second)
			} else {
				time.Sleep(time.Duration(secondsPastSix) * time.Second)
			}
			if err := m.datastore.UpdateActiveValidators(); err != nil {
				m.log.WithError(err).Error("could not update active validator count")
			}
		}
	}()
}

func (m *BoostService) isKnownValidator(pubkey string) bool {
	_, ok := m.validators.Load(pubkey)
	return ok
}

func (m *BoostService) checkRegisterValidator(r *http.Request) bool {
	var payloads []types.SignedValidatorRegistration
	if err := decodeJSON(r.Body, &payloads); err != nil {
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

func (m *BoostService) checkGetHeaderValidator(r *http.Request) bool {
	vars := mux.Vars(r)
	pubkey := vars["pubkey"]
	return m.isKnownValidator(pubkey)
}

func (m *BoostService) fetchCurrentEpoch() (int, error) {

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

func (m *BoostService) parseRelayURLs(relayURLs string) []RelayEntry {
	var ret []RelayEntry
	for _, entry := range strings.Split(relayURLs, ",") {
		relay, err := NewRelayEntry(entry)
		if err != nil {
			m.log.WithError(err).WithField("relayURL", entry).Fatal("invalid relay URL")
		}
		ret = append(ret, relay)
	}
	return ret
}

func (m *BoostService) verifyBuilderBlockSubmission(payload *capellaapi2.SubmitBlockRequest) error {
	if payload.Message.BlockHash.String() != payload.ExecutionPayload.BlockHash.String() {
		return errors.New("blockHash mismatch")
	}

	if payload.Message.ParentHash.String() != payload.ExecutionPayload.ParentHash.String() {
		return errors.New("parentHash mismatch")
	}

	m.expectedPayloadDataLock.RLock()
	expectedRandao := m.expectedPrevRandao
	m.expectedPayloadDataLock.RUnlock()
	if expectedRandao.prevRandao != fmt.Sprintf("%#x", payload.ExecutionPayload.PrevRandao) {
		return fmt.Errorf("incorrect prev_randao - got: %s, expected: %s", fmt.Sprintf("%#x", payload.ExecutionPayload.PrevRandao), expectedRandao.prevRandao)
	}

	withdrawalsRoot, err := ComputeWithdrawalsRoot(payload.ExecutionPayload.Withdrawals)
	if err != nil {
		m.log.WithError(err).Warn("could not compute withdrawals root from payload")
		return err
	}
	if m.nextSlotWithdrawalsRoot != withdrawalsRoot {
		return fmt.Errorf("incorrect withdrawals root - got: %s, expected: %s", withdrawalsRoot.String(), m.nextSlotWithdrawalsRoot.String())
	}

	return nil
}

func ComputeWithdrawalsRoot(w []*capella.Withdrawal) (phase0.Root, error) {
	withdrawals := utilcapella.ExecutionPayloadWithdrawals{Withdrawals: w}
	return withdrawals.HashTreeRoot()
}

func (m *BoostService) saveBlock(
	ctx context.Context,
	getHeaderResponse common.GetHeaderResponse,
	submission capellaapi2.SubmitBlockRequest,
	signedBidTrace *v1.BidTrace,
	receivedAt time.Time,
	log *logrus.Entry,
	tier sdnmessage.AccountTier,
) {
	start := time.Now().UTC()

	go func() {
		if err := m.datastore.SaveBuilderBlockHash(ctx, getHeaderResponse.Capella.Capella.Message.Header.BlockHash.String(), submission.Message.BuilderPubkey.String()); err != nil {
			m.log.WithError(err).Warn("could not save builder block hash")
		}
	}()

	isMostProfitable, saveBlockStats, err := m.datastore.SaveBlock(
		ctx,
		&getHeaderResponse,
		submission.ExecutionPayload,
		signedBidTrace,
		receivedAt,
		m.keysToExpireChan,
		tier,
	)
	if err != nil {
		log.WithError(err).Error("could not save bid")
		return
	}

	// TODO: Should we move this before the bid is saved? Or possibly right after signature is validated in 'handlePostBlock()'?
	// broadcast the block to Redis 'blockSubmission' channel subscribers
	go func() {
		err := m.datastore.RedisPublish(blockSubmissionChannel, submission, m.nodeUUID.String())
		if err != nil {
			log.WithError(err).Error("could not broadcast payload to Redis subscribers")
		}
	}()

	totalDuration := time.Since(start).Milliseconds()

	log.WithFields(logrus.Fields{
		"totalDurationSinceReceived": time.Since(receivedAt).Milliseconds(),
		"saveBlockStartTime":         start.String(),
		"totalSaveToRedisDuration":   totalDuration,
		"saveToRedisStats":           fmt.Sprintf("%+v", *saveBlockStats),
	}).Info("bid saved and available on redis")

	if m.stats.NodeID != "" {
		isActive := false
		if validatorData, err := m.datastore.GetActiveValidator(ctx, signedBidTrace.ProposerPubkey.String()); err != nil || validatorData == nil {
			isActive = false
		} else {
			isActive = true
		}

		stat := bidSavedLog{
			Duration:          totalDuration,
			ValidatorIsActive: isActive,
			BlockHash:         submission.ExecutionPayload.BlockHash.String(),
			BuilderPubKey:     signedBidTrace.BuilderPubkey.String(),
			Slot:              signedBidTrace.Slot,
		}
		record := statistics.Record{
			Data: &stat,
			Type: "BidSavedToRedis",
		}
		m.stats.LogToFluentD(record, time.Now(), "stats.save_block_bid_saved_to_redis")
	}

	// Save block submission to database with retry mechanism
	go m.saveBuilderBlockSubmissionToDB(ctx, &submission, signedBidTrace, isMostProfitable, receivedAt, log)

	// keep track of best bloXroute-built bid for this slot
	if m.isBloxrouteBlock(submission.ExecutionPayload.ExtraData) {
		mostProfitableBloxrouteBid, found := m.bestBloxrouteBlockBidForSlot.Load(submission.Message.Slot)
		if !found || signedBidTrace.Value.Cmp(mostProfitableBloxrouteBid.Value) > 0 {
			m.bestBloxrouteBlockBidForSlot.Store(submission.Message.Slot, *signedBidTrace)
		}
	}

	if isMostProfitable {
		m.bestBlockBidForSlot.Store(submission.Message.Slot, *signedBidTrace)
	}

	if m.cloudServicesEndpoint == "" {
		return
	}

	payload := blockValuePayload{
		Method: string(RPCMEVBlock),
		Params: blockValuePayloadParams{
			RelayType:            m.relayType,
			SlotNumber:           submission.Message.Slot,
			BlockNumber:          submission.ExecutionPayload.BlockNumber,
			BlockHash:            submission.ExecutionPayload.BlockHash.String(),
			BlockValue:           getHeaderResponse.Capella.Capella.Message.Value.ToBig(),
			ProposerFeeRecipient: signedBidTrace.ProposerFeeRecipient.String(),
			GasUsed:              getHeaderResponse.Capella.Capella.Message.Header.GasUsed,
			BuilderPubkey:        signedBidTrace.BuilderPubkey.String(),
			ParentHash:           submission.ExecutionPayload.ParentHash.String(),
			TimestampMs:          time.Now().UTC().UnixMilli(),
		},
	}
	go m.SendCloudServicesHTTPRequest(ctx, m.cloudServicesHttpClient, payload, nil)
}

func SignedBlindedBeaconBlockToBeaconBlock(signedBlindedBeaconBlock *capellaapi.SignedBlindedBeaconBlock, executionPayload *capella.ExecutionPayload) *capella.SignedBeaconBlock {
	var signedBeaconBlock capella.SignedBeaconBlock

	if signedBlindedBeaconBlock != nil {
		signedBeaconBlock = capella.SignedBeaconBlock{
			Signature: signedBlindedBeaconBlock.Signature,
			Message: &capella.BeaconBlock{
				Slot:          signedBlindedBeaconBlock.Message.Slot,
				ProposerIndex: signedBlindedBeaconBlock.Message.ProposerIndex,
				ParentRoot:    signedBlindedBeaconBlock.Message.ParentRoot,
				StateRoot:     signedBlindedBeaconBlock.Message.StateRoot,
				Body: &capella.BeaconBlockBody{
					BLSToExecutionChanges: signedBlindedBeaconBlock.Message.Body.BLSToExecutionChanges,
					RANDAOReveal:          signedBlindedBeaconBlock.Message.Body.RANDAOReveal,
					ETH1Data:              signedBlindedBeaconBlock.Message.Body.ETH1Data,
					Graffiti:              signedBlindedBeaconBlock.Message.Body.Graffiti,
					ProposerSlashings:     signedBlindedBeaconBlock.Message.Body.ProposerSlashings,
					AttesterSlashings:     signedBlindedBeaconBlock.Message.Body.AttesterSlashings,
					Attestations:          signedBlindedBeaconBlock.Message.Body.Attestations,
					Deposits:              signedBlindedBeaconBlock.Message.Body.Deposits,
					VoluntaryExits:        signedBlindedBeaconBlock.Message.Body.VoluntaryExits,
					SyncAggregate:         signedBlindedBeaconBlock.Message.Body.SyncAggregate,
					ExecutionPayload:      executionPayload,
				},
			},
		}
	}

	return &signedBeaconBlock
}

func (m *BoostService) GetPayloadResponseToBlockSubmission(payload *common.GetPayloadResponse) (*common.BuilderSubmitBlockRequest, error) {

	bidtrace, err := m.datastore.GetBidTrace(context.Background(), payload.Capella.Capella.BlockHash.String())
	if err != nil {
		return nil, err
	}

	pubKey := types.PublicKey{}
	pubKeySlice := ethCommon.Hex2Bytes(bidtrace.BuilderPubkey.String()[2:])
	if err := pubKey.FromSlice(pubKeySlice); err != nil {
		m.log.Warn("pubkey hex isn't able to be a pub key")
		return nil, err
	}

	proposerPubKey := types.PublicKey{}
	proposerPubKeySlice := ethCommon.Hex2Bytes(bidtrace.ProposerPubkey.String()[2:])
	if err := pubKey.FromSlice(proposerPubKeySlice); err != nil {
		m.log.Warn("pubkey hex isn't able to be a pub key")
		return nil, err
	}

	feeRecipient, err := types.HexToAddress(bidtrace.ProposerFeeRecipient.String())
	if err != nil {
		return nil, err
	}
	if err := pubKey.FromSlice(proposerPubKeySlice); err != nil {
		return nil, err
	}

	sig, err := types.SignMessage(bidtrace, m.builderSigningDomain, m.secretKey)
	if err != nil {
		return nil, err
	}

	value := new(uint256.Int)
	if ok := value.SetFromBig(value.ToBig()); !ok {
		m.log.Error("could not set block value")
		return nil, err
	}

	res := common.BuilderSubmitBlockRequest{
		Capella: &capellaapi2.SubmitBlockRequest{
			Message: &v1.BidTrace{
				Slot:                 bidtrace.Slot,
				ParentHash:           phase0.Hash32(payload.Capella.Capella.ParentHash),
				BlockHash:            phase0.Hash32(payload.Capella.Capella.BlockHash),
				BuilderPubkey:        phase0.BLSPubKey(pubKey),
				ProposerPubkey:       phase0.BLSPubKey(proposerPubKey),
				ProposerFeeRecipient: bellatrix.ExecutionAddress(feeRecipient),
				GasLimit:             bidtrace.GasLimit,
				GasUsed:              bidtrace.GasUsed,
				Value:                value,
			},
			ExecutionPayload: payload.Capella.Capella,
			Signature:        phase0.BLSSignature(sig),
		},
	}

	return &res, nil
}

func (m *BoostService) saveBuilderBlockSubmissionToDB(ctx context.Context, submission *capellaapi2.SubmitBlockRequest, signedBidTrace *v1.BidTrace, isMostProfitable bool, receivedAt time.Time, log *logrus.Entry) {
	// Save block submission to database with retry mechanism
	if m.db != nil {
		b := backoff.NewExponentialBackOff()
		b.MaxElapsedTime = saveBlockToDatabaseMaxElapsedTime
		b.MaxInterval = saveBlockToDatabaseMaxInterval

		err := backoff.Retry(func() error {
			ctx, cancel := context.WithTimeout(ctx, defaultDatabaseRequestTimeout)
			defer cancel()
			_, err := m.db.SaveBuilderBlockSubmissionToDB(ctx, submission, nil, isMostProfitable, receivedAt)
			if err != nil {
				log.WithError(err).Error("failed attempt to save builder block submission to database")
			}
			return err
		}, b)
		if err != nil {
			log.WithError(err).Error("failed to save builder block submission to database")
			return
		}
		log.Info("successfully saved builder block submission to database")
	}
}

func (m *BoostService) StartSaveBlocksPubSub() {
	m.log.Infof("subscribing to Redis pubsub '%s' channel", blockSubmissionChannel)
	go m.datastore.RedisSubscribe(blockSubmissionChannel, m.redisBlockSubChannel)

	for {
		select {
		// new payload attributes slot number received, cancel outstanding saves of bids for the previous slot
		case newPayloadAttributesSlotNumber := <-m.newPayloadAttributesSlotChannel:
			go func() {
				previousSlot := newPayloadAttributesSlotNumber - 1
				err := m.cancelAllBuilderContextsForSlot(previousSlot)
				if err != nil {
					m.log.WithError(err).Errorf("new payload attributes slot %v received, failed to cancel saving of bids for previous slot %v", newPayloadAttributesSlotNumber, previousSlot)
					return
				}
				m.log.Infof("new payload attributes slot %v received, successfully canceled saving of bids for previous slot %v", newPayloadAttributesSlotNumber, previousSlot)
			}()

		// new block from other relay
		case redisMsg := <-m.redisBlockSubChannel:
			go func() {
				payload := &common.WrappedCapellaBuilderSubmitBlockRequest{}
				senderUUID, err := m.datastore.UnmarshalRedisCapellaBlockMessage(redisMsg, payload)
				if err != nil {
					m.log.WithError(err).Errorf("could not unmarshal blockSubmission subscription message from Redis, message: %s", redisMsg.Payload)
					return
				}

				builderPubKey := payload.Payload.Message.BuilderPubkey.String()
				slot := payload.Payload.Message.Slot

				// only cancel saving for this block if current save is in progress
				// and sender of the Redis message was an external node
				builderCtx, ok := m.getBuilderContextForSlot(builderPubKey, slot)
				if ok && senderUUID != m.nodeUUID.String() {
					blockHash := payload.Payload.Message.BlockHash.String()
					newBlockReceiveTime := payload.ReceiveTime

					// only cancel bid saving if the new block received from Redis was received after the
					// local block AND the new block from Redis has a higher value than the local block
					shouldCancelBidSave := newBlockReceiveTime.After(builderCtx.BidReceivedTime) &&
						payload.Payload.Message.Value.ToBig().Cmp(builderCtx.BidValue) > 0

					if shouldCancelBidSave {
						m.log.Infof("canceling saving of bid with block hash %s and value %s with received time of %s from builder %s for slot %v due to blockSubmission subscription message from Redis with block hash %s and value %s received at %s", builderCtx.BlockHash, common.WeiToEth(builderCtx.BidValue.String()), builderCtx.BidReceivedTime, builderPubKey, slot, blockHash, common.WeiToEth(payload.Payload.Message.Value.ToBig().String()), payload.ReceiveTime)
						builderCtx.Cancel()
					}
				}
			}()

		// new block from this relay
		case newBlock := <-m.newBlockChannel:
			go func() {

				newBlockHash := newBlock.submission.Payload.Message.BlockHash.String()
				builderPubKey := newBlock.submission.Payload.Message.BuilderPubkey.String()
				slot := newBlock.submission.Payload.Message.Slot

				if builderCtx, found := m.getBuilderContextForSlot(builderPubKey, slot); found {
					// only cancel in-progress bid saving if this new block was received after the
					// in-progress bid AND the new block has a higher value than the in-progress bid
					shouldCancelBidSave := newBlock.receivedAt.After(builderCtx.BidReceivedTime) &&
						newBlock.signedBidTrace.Value.ToBig().Cmp(builderCtx.BidValue) > 0

					if shouldCancelBidSave {
						m.log.Infof("canceling saving of bid with block hash %s and value %s with received time of %s from builder %s for slot %v due to builder bid with block hash %s and value %s received at %s", builderCtx.BlockHash, common.WeiToEth(builderCtx.BidValue.String()), builderCtx.BidReceivedTime, builderPubKey, slot, newBlockHash, common.WeiToEth(newBlock.signedBidTrace.Value.ToBig().String()), newBlock.receivedAt)
						builderCtx.Cancel()
					}
				}

				builderSpecificContext, newCancelFunc := context.WithCancel(context.Background())
				newBuilderCtx := &builderContextData{
					Ctx:             builderSpecificContext,
					Cancel:          newCancelFunc,
					BidReceivedTime: newBlock.receivedAt,
					BidValue:        newBlock.signedBidTrace.Value.ToBig(),
					BlockHash:       newBlockHash,
				}
				m.setBuilderContextForSlot(builderPubKey, slot, newBuilderCtx)

				log := m.createBlockSaveLog(
					builderPubKey,
					slot,
					newBlock.signedBidTrace.Value.String(),
					newBlock.submission.Payload.ExecutionPayload.BlockHash.String(),
					newBlock.receivedAt.String(),
					newBlock.simulationTime,
					newBlock.clientIPAddress,
					newBlock.reqRemoteAddress,
				)

				// TODO: test performance as-is vs. changing this to not be a goroutine
				go m.saveBlock(
					newBuilderCtx.Ctx,
					newBlock.getHeaderResponse,
					*newBlock.submission.Payload,
					newBlock.signedBidTrace,
					newBlock.receivedAt,
					log,
					newBlock.tier,
				)
			}()
		}
	}
}

func (m *BoostService) setBuilderContextForSlot(builderPubKey string, slot uint64, builderCtx *builderContextData) {
	if !m.builderContextsForSlot.Has(slot) {
		m.builderContextsForSlot.Store(slot, syncmap.NewStringMapOf[*builderContextData]())
	}
	builderContextMap, _ := m.builderContextsForSlot.Load(slot)
	builderContextMap.Store(builderPubKey, builderCtx)
}

func (m *BoostService) getBuilderContextForSlot(builderPubKey string, slot uint64) (*builderContextData, bool) {
	if builderContextMap, slotFound := m.builderContextsForSlot.Load(slot); slotFound {
		builderCtx, found := builderContextMap.Load(builderPubKey)
		return builderCtx, found
	}
	return nil, false
}

func (m *BoostService) cancelAllBuilderContextsForSlot(slot uint64) error {
	if builderContextMap, found := m.builderContextsForSlot.Load(slot); found {
		builderContextMap.Range(func(key string, ctx *builderContextData) bool {
			ctx.Cancel()
			return true
		})

		return nil
	}
	return fmt.Errorf("no builder context map found for slot %v", slot)
}

func convertToIntegerSyncmap[K xsync.IntegerConstraint, V any](rawMap map[K]V) *syncmap.SyncMap[K, V] {
	newMap := syncmap.NewIntegerMapOf[K, V]()
	for key, value := range rawMap {
		newMap.Store(key, value)
	}
	return newMap
}

func convertToStringSyncmap[V any](rawMap map[string]V) *syncmap.SyncMap[string, V] {
	newMap := syncmap.NewStringMapOf[V]()
	for key, value := range rawMap {
		newMap.Store(key, value)
	}
	return newMap
}

func (m *BoostService) createBlockSaveLog(builderPubKey string, slot uint64, value string, blockHash string, receiveTime string, simulationTime string, clientIPAddress string, reqRemoteAddress string) *logrus.Entry {
	return m.log.WithFields(logrus.Fields{
		"builderPubkey":    builderPubKey,
		"slot":             slot,
		"value":            value,
		"blockHash":        blockHash,
		"receiveTime":      receiveTime,
		"simulationTime":   simulationTime,
		"clientIPAddress":  clientIPAddress,
		"reqRemoteAddress": reqRemoteAddress,
	})
}
