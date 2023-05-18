package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/attestantio/go-builder-client/api/capella"
	"github.com/attestantio/go-builder-client/spec"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/mev-relay/datastore"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/bloXroute-Labs/mev-relay/common"
)

// UserAgent is a custom string type to avoid confusing url + userAgent parameters in SendHTTPRequest
type UserAgent string

// SendHTTPRequest - prepare and send HTTP request, marshaling the payload if any, and decoding the response if dst is set
func SendHTTPRequest(ctx context.Context, client http.Client, method, url string, userAgent UserAgent, payload any, dst any) (code int, err error) {
	var req *http.Request

	if payload == nil {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		if err2 != nil {
			return 0, fmt.Errorf("could not marshal request: %w", err2)
		}
		req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payloadBytes))

		// Set content-type
		req.Header.Add("Content-Type", "application/json")
	}
	if err != nil {
		return 0, fmt.Errorf("could not prepare request: %w", err)
	}

	// Set user agent
	req.Header.Set("User-Agent", strings.TrimSpace(fmt.Sprintf("mev-boost/%s %s", Version, userAgent)))

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

// ComputeDomain computes the signing domain
func ComputeDomain(domainType types.DomainType, forkVersionHex string, genesisValidatorsRootHex string) (domain types.Domain, err error) {
	genesisValidatorsRoot := types.Root(ethcommon.HexToHash(genesisValidatorsRootHex))
	forkVersionBytes, err := hexutil.Decode(forkVersionHex)
	if err != nil || len(forkVersionBytes) > 4 {
		err = errors.New("invalid fork version passed")
		return domain, err
	}
	var forkVersion [4]byte
	copy(forkVersion[:], forkVersionBytes[:4])
	return types.ComputeDomain(domainType, forkVersion, genesisValidatorsRoot), nil
}

// decodeJSON reads JSON from io.Reader and decodes it into a struct
func decodeJSON(r io.Reader, dst any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		return err
	}
	return nil
}

// decodeAndCloseJSON reads JSON from io.ReadCloser, decodes it into a struct and
// closes the reader
func decodeJSONAndClose(r io.ReadCloser, dst any) error {
	defer r.Close()
	return decodeJSON(r, dst)
}

func checkBLSPublicKeyHex(pkHex string) error {
	var proposerPubkey types.PublicKey
	return proposerPubkey.UnmarshalText([]byte(pkHex))
}

func (m *BoostService) handleBlockPayload(ctx context.Context, log *logrus.Entry, payload *capella.SubmitBlockRequest, stat *builderBlockReceivedStatsRecord, externalBuilderAccountID string, isExternalBuilder bool, start time.Time, clientIPAddress, reqRemoteAddress string, receivedAt time.Time, tier sdnmessage.AccountTier) error {
	localContext, handleBlockPayloadSpan := m.tracer.Start(ctx, "handleBlockPayload")
	defer handleBlockPayloadSpan.End()
	sanityContext, sanitySpan := m.tracer.Start(localContext, "sanityChecks")

	slot := payload.Message.Slot

	blockHash := payload.Message.BlockHash.String()

	builderPubkey := payload.Message.BuilderPubkey.String()

	log = log.WithFields(logrus.Fields{
		"slot":      slot,
		"blockHash": blockHash,
	})

	if payload.Message.ProposerFeeRecipient.String() == emptyWalletAddressZero {
		stat.HttpResponseCode = http.StatusPreconditionFailed
		errMessage := "skipping due to empty fee recipient address"
		log.Trace(errMessage)
		return errors.New(errMessage)
	}

	if slot <= m.latestSlotBlockReceived.Load() {
		stat.HttpResponseCode = http.StatusConflict
		errMessage := "skipping due to payload for old slot"
		log.WithField("latestSlotBlockReceived", m.latestSlotBlockReceived.Load()).Trace(errMessage)
		return errors.New(errMessage)
	}

	log = log.WithFields(logrus.Fields{
		"builderPubKey": builderPubkey,
		"value":         payload.Message.Value.String(),
		"ethValue":      common.WeiToEthRounded(payload.Message.Value.ToBig().String()),
		"blockNumber":   payload.ExecutionPayload.BlockNumber,
	})

	expectedTimestamp := m.genesisInfo.Data.GenesisTime + (slot * 12)
	if payload.ExecutionPayload.Timestamp != expectedTimestamp {
		stat.HttpResponseCode = http.StatusBadRequest
		errMessage := "execution payload timestamp did not match expected timestamp"
		return errors.New(errMessage)
	}

	// ensure correct feeRecipient is used
	slotDuty, ok := m.proposerDutiesMap.Load(payload.Message.Slot)
	if slotDuty == nil || !ok {
		log.Warn("could not find slot duty")
		stat.HttpResponseCode = http.StatusBadRequest
		errMessage := "could not find slot duty"
		return errors.New(errMessage)
	} else if !strings.EqualFold(slotDuty.FeeRecipient.String(), payload.Message.ProposerFeeRecipient.String()) {
		log.Infof("fee recipient does not match.  Expected: %s, got: %s", slotDuty.FeeRecipient.String(), payload.Message.ProposerFeeRecipient.String())
		stat.HttpResponseCode = http.StatusBadRequest
		errMessage := "fee recipient does not match"
		return errors.New(errMessage)
	}

	status, err := m.datastore.GetSetBlockSubmissionStatus(sanityContext, blockHash, datastore.BlockSimulationSubmitted)
	if err != nil && err != redis.Nil {
		stat.HttpResponseCode = http.StatusInternalServerError
		return err
	}

	if status != "" {
		if status == datastore.BlockSimulationFailed {
			stat.HttpResponseCode = http.StatusNotAcceptable
			return errors.New("block already failed simulation")
		}

		if status == datastore.BlockSimulationPassed || status == datastore.BlockSimulationSubmitted {
			stat.HttpResponseCode = http.StatusAlreadyReported
			return errors.New("block already received")
		}
	}

	builderPubkeySkipSimulationThreshold, found := m.highPriorityBuilderPubkeysToSkipSimulationThreshold.Load(builderPubkey)
	if !found {
		builderPubkeySkipSimulationThreshold = defaultBuilderPubkeySkipSimulationThreshold
	}

	builderAccountIDSkipSimulationThreshold, found := m.highPriorityBuilderAccountIDsToSkipSimulationThreshold.Load(externalBuilderAccountID)
	if !found {
		builderAccountIDSkipSimulationThreshold = defaultBuilderAccountIDSkipSimulationThreshold
	}

	_, builderDemoted := m.demotedBuilders.Load(builderPubkey)
	isBuilderPubkeyHighPriority, _ := m.highPriorityBuilderPubkeys.Load(builderPubkey)
	isBuilderAccountIDHighPriority, _ := m.highPriorityBuilderAccountIDs.Load(externalBuilderAccountID)

	// TODO: remove coinbaseExchangeWalletAddress check when we fix builder's block value calculation issue
	isBuilderPubkeyQualifiedForSkipSimulation := isBuilderPubkeyHighPriority && payload.Message.Value.ToBig().Cmp(builderPubkeySkipSimulationThreshold) < 0 && payload.Message.ProposerFeeRecipient.String() != coinbaseExchangeWalletAddress
	isBuilderAccountIDQualifiedForSkipSimulation := isBuilderAccountIDHighPriority && payload.Message.Value.ToBig().Cmp(builderAccountIDSkipSimulationThreshold) < 0
	skipSimulation := isBuilderAccountIDQualifiedForSkipSimulation || isBuilderPubkeyQualifiedForSkipSimulation
	if builderDemoted {
		skipSimulation = false
		log.WithField("builderPubKey", builderPubkey).Info("not skipping simulation for demoted builder")
	}

	currentSimulationNodes := m.simulationNodes
	isHighPerfSimBuilder, _ := m.highPerfSimBuilderPubkeys.Load(builderPubkey)
	isUltraBuilder, _ := m.ultraBuilderAccountIDs.Load(externalBuilderAccountID)
	isBuilderEligibleForHighPerfNode := isHighPerfSimBuilder || isUltraBuilder

	if isBuilderEligibleForHighPerfNode && m.simulationNodeHighPerf != "" {
		currentSimulationNodes = append(currentSimulationNodes, m.simulationNodeHighPerf)
	}

	res := make(chan simulationResult, len(currentSimulationNodes))

	simContext, simSpan := m.tracer.Start(localContext, "blockSimulation")
	if !skipSimulation {
		simPayload := &common.BuilderBlockValidationRequest{
			BuilderSubmitBlockRequest: common.BuilderSubmitBlockRequest{Capella: payload},
			RegisteredGasLimit:        slotDuty.GasLimit,
		}
		go m.simulate(simContext, log, simPayload, currentSimulationNodes, res)
	} else {
		// close the channel to avoid deadlock
		close(res)
		simSpan.End()
	}

	log.Info("submitting block payload")

	// Sanity check the submission
	err = m.verifyBuilderBlockSubmission(payload)
	if err != nil {
		stat.HttpResponseCode = http.StatusBadRequest
		return err
	}

	// don't verify signature for internal builder (whitelisted by ip)
	if isExternalBuilder {
		ok, err := types.VerifySignature(payload.Message, m.builderSigningDomain, payload.Message.BuilderPubkey[:], payload.Signature[:])
		if !ok || err != nil {
			stat.HttpResponseCode = http.StatusBadRequest
			if err != nil {
				log = log.WithError(err)
			}
			return err
		}
	}

	sanitySpan.End()

	_, responseBuildingSpan := m.tracer.Start(localContext, "responseBuilding")

	header, err := common.PayloadToPayloadHeader(payload.ExecutionPayload)
	if err != nil {
		log.Fatal(err)
	}

	builderBid := capella.BuilderBid{
		Value:  payload.Message.Value,
		Header: header,
		Pubkey: (phase0.BLSPubKey)(m.pubKey),
	}

	sig, err := types.SignMessage(&builderBid, m.builderSigningDomain, m.secretKey)
	if err != nil {
		log.WithError(err).Fatal("could not sign message")
	}

	signedBuilderBid := capella.SignedBuilderBid{
		Message:   &builderBid,
		Signature: (phase0.BLSSignature)(sig),
	}

	responseBuildingSpan.End()

	_, simResponseSpan := m.tracer.Start(simContext, "simulationResponseWait")
	// if skipSimulation is true, then res channel will be closed and won't block further execution
	simulationResult, ok := <-res
	simResponseSpan.End()
	simSpan.End()
	if ok {
		// add simulation time to stat even if simulation failed
		stat.SimulationTime = simulationResult.duration.String()

		// non-nil fastestNodeURL means simulation succeeded
		if simulationResult.fastestNodeURL != nil {
			stat.SimulationNodeURL = *simulationResult.fastestNodeURL
		} else {
			stat.HttpResponseCode = http.StatusNotAcceptable
			// no need to log here, since there are separate logs per simulation node
			return err
		}
	}

	// Ensure this request is still the latest one. This logic intentionally
	// ignores the value of the bids and makes the current active bid the one
	// that arrived at the relay last. This allows for builders to reduce the
	// value of their bid (effectively cancel a high bid) by ensuring a lower
	// bid arrives later. Even if the higher bid takes longer to simulate,
	// by checking the receivedAt timestamp, this logic ensures that the low bid
	// is not overwritten by the high bid.
	//
	// NOTE: this can lead to a rather tricky race condition. If a builder
	// submits two blocks to the relay concurrently, then the randomness of
	// network latency will make it impossible to predict which arrives first.
	// Thus, a high bid could unintentionally be overwritten by a low bid that
	// happened to arrive a few microseconds later. If builders are submitting
	// blocks at a frequency where they cannot reliably predict which bid will
	// arrive at the relay first, they should instead use multiple pubkeys to
	// avoid unintentionally overwriting their own bids.
	latestPayloadReceivedAt, err := m.datastore.GetBuilderLatestPayloadReceivedAt(localContext, payload.Message.Slot, builderPubkey, payload.Message.ParentHash.String(), payload.Message.ProposerPubkey.String())
	if err != nil {
		log.WithError(err).Error("failed getting latest payload receivedAt from redis")
	} else if start.UnixMilli() < latestPayloadReceivedAt {
		log.Infof("already have a newer payload: now=%d / prev=%d", start.UnixMilli(), latestPayloadReceivedAt)
		return err
	}

	getHeaderResponse := common.GetHeaderResponse{
		Capella: &spec.VersionedSignedBuilderBid{
			Capella: &signedBuilderBid,
			Version: consensusspec.DataVersionCapella,
		},
	}
	if m.enableBidSaveCancellation {
		saveBlockData := &saveBlockData{
			getHeaderResponse: getHeaderResponse,
			submission: common.WrappedCapellaBuilderSubmitBlockRequest{
				Payload:     payload,
				ReceiveTime: receivedAt,
			},
			signedBidTrace:   payload.Message,
			receivedAt:       start,
			clientIPAddress:  clientIPAddress,
			reqRemoteAddress: reqRemoteAddress,
			simulationTime:   stat.SimulationTime,
			tier:             tier,
		}

		// send the block data to channel to be saved in Redis and database
		m.newBlockChannel <- saveBlockData

	} else {
		_, createSaveLogSpan := m.tracer.Start(localContext, "createSaveLog")
		saveBlockLog := m.createBlockSaveLog(
			payload.Message.BuilderPubkey.String(),
			slot,
			payload.Message.Value.String(),
			payload.ExecutionPayload.BlockHash.String(),
			start.String(),
			stat.SimulationTime,
			clientIPAddress,
			reqRemoteAddress,
		)
		createSaveLogSpan.End()

		go m.saveBlock(
			localContext,
			getHeaderResponse,
			*payload,
			payload.Message,
			start,
			saveBlockLog,
			tier,
		)
	}

	_, finishing := m.tracer.Start(localContext, "finishing")
	defer finishing.End()

	go func() {
		err = m.datastore.SaveBidTrace(localContext, payload.Message, blockHash)
		if err != nil {
			log.WithError(err).Error("failed saving bidTrace in redis")
			return
		}
	}()

	logFields := logrus.Fields{
		"parentHash":     payload.Message.ParentHash.String(),
		"proposerPubkey": payload.Message.ProposerPubkey.String(),
		"tx":             len(payload.ExecutionPayload.Transactions),
	}

	if isExternalBuilder {
		logFields["accountID"] = externalBuilderAccountID
	}

	log.WithFields(logFields).Trace("new block from builder accepted")

	// Respond with OK

	go func() {
		if _, ok := m.providedHeaders.Load(int64(slot)); !ok {
			m.providedHeaders.Store(int64(slot), ProvidedHeaders{
				Headers:           make([]*common.GetHeaderResponse, 0),
				ProposerPublicKey: payload.Message.ProposerPubkey.String(),
			})
		}
	}()

	stat.HttpResponseCode = http.StatusOK
	return nil
}
