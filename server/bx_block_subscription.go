package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/bloXroute-Labs/gateway/v2/services/statistics"
	"github.com/bloXroute-Labs/mev-relay/beaconclient"
	"github.com/bloXroute-Labs/mev-relay/common"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	slotCleanupInterval       = 5
	secondsPerSlot      int64 = 12
)

type beaconBlockResponse struct {
	Data beaconBlockData `json:"data"`
}

type beaconBlockData struct {
	Message beaconBlockMessage `json:"message"`
}

type beaconBlockMessage struct {
	Body          beaconBlock `json:"body"`
	Slot          string      `json:"slot"`
	ProposerIndex string      `json:"proposer_index"`
}

type blockStat struct {
	ProposedBlockMessage beaconBlockMessage
	BXBlockData          builderBestHeaderPayload
	WasBXProposed        bool
	ConfirmedBlockProfit *big.Int
	ProvidedHeaders      []*common.GetHeaderResponse
	BestProvidedHeader   *common.GetHeaderResponse
	BestHeader           *common.GetHeaderResponse
	HeaderProvided       bool
	PayloadProvided      bool
	ProposerRegistered   bool
}

type beaconBlock struct {
	ExecutionPayload types.ExecutionPayload `json:"execution_payload"`
}

type builderBestHeaderPayload struct {
	Header  capella.ExecutionPayloadHeader `json:"header"`
	Payload capella.ExecutionPayload       `json:"payload"`
	Profit  *big.Int                       `json:"profit"`
}

type bestBuilderBlockForSlotStat struct {
	Slot                 uint64
	SlotStartTime        string
	BlockHash            string
	ParentHash           string
	ProposerPubkey       string
	Value                string
	EthValue             string
	BestBlocksFromRelays map[string]BestBlockFromRelay
}

type BestBlockFromRelay struct {
	BlockHash     string
	ParentHash    string
	BlockValueETH string
	BlockValueWEI string
}

// StartSendNextSlotProposerDuties runs a loop to send next slot proposer duties to cloud services on slot start time
func (m *BoostService) StartSendNextSlotProposerDuties() {
	currentSlot := ((time.Now().UTC().Unix() - int64(m.genesisInfo.Data.GenesisTime)) / secondsPerSlot) + 1
	slotStartTime := time.Unix(int64(m.genesisInfo.Data.GenesisTime)+(currentSlot*secondsPerSlot), 0)

	for {
		slotStartTime = slotStartTime.Add(time.Duration(secondsPerSlot) * time.Second)
		currentSlot++
		time.Sleep(time.Until(slotStartTime))
		m.sendNextSlotProposerDuties(uint64(currentSlot))
	}
}

// sendNextSlotProposerDuties sends the next slot's proposer info to cloud services
func (m *BoostService) sendNextSlotProposerDuties(currentSlot uint64) {
	if !m.sendSlotProposerDuties {
		return
	}
	nextSlot := currentSlot + 1
	proposerDutiesForNextSlot, ok := m.allProposerDuties.Load(nextSlot)
	if !ok {
		m.log.Errorf("could not retrieve proposer duties for next slot %v", nextSlot)
	} else {
		if m.cloudServicesEndpoint != "" {
			payload := slotProposerPayload{
				Method: string(RPCProposerSlot),
				Params: slotProposerPayloadParams{
					SlotNumber:        nextSlot,
					ProposerIndex:     proposerDutiesForNextSlot.Index,
					ProposerPublicKey: proposerDutiesForNextSlot.Pubkey,
				},
			}
			m.log.Infof("sending next slot %v proposer duties to cloud services", nextSlot)
			go m.SendCloudServicesHTTPRequest(context.Background(), m.cloudServicesHttpClient, payload, nil)
		}
	}
}

// SubscribeToBlocks subscribes to new block events
// from the beacon node
func (m *BoostService) SubscribeToBlocks() {
	go func() {
		c := make(chan beaconclient.HeadEventData)
		m.beaconClient.SubscribeToHeadEvents(c)
		for {
			headEvent := <-c
			m.processNewSlot(headEvent.Slot)
		}
	}()
}

// SubscribeToPayloadAttributes subscribes to new block events
// from the beacon node
func (m *BoostService) SubscribeToPayloadAttributes() {
	go func() {
		c := make(chan beaconclient.PayloadAttributesData)
		m.beaconClient.SubscribeToPayloadAttributes(c)
		for {
			payloadAttributesEvent := <-c
			m.processNewPayloadAttributes(payloadAttributesEvent)
		}
	}()
}

func (m *BoostService) processNewSlot(headSlot uint64) {
	m.log.WithField("headSlot", headSlot).Info("new head slot")
	prevHeadSlot := m.latestSlotBlockReceived.Load()
	if headSlot <= prevHeadSlot {
		return
	}

	newSlotTime := time.Now().UTC()
	m.latestSlotBlockReceived.Store(headSlot)

	go func() {
		demotedBuilders, err := m.datastore.GetDemotedBuilderPubkeys(context.Background())
		if err != nil && err != redis.Nil {
			m.log.Warn("could not fetch demoted builders")
			return
		}

		// TODO: not sure if we still need lock to just copy pointer?
		m.demotedBuilderLock.Lock()
		m.demotedBuilders = demotedBuilders
		m.demotedBuilderLock.Unlock()
	}()

	if prevHeadSlot > 0 {
		if m.stats.NodeID != "" {
			m.logSlotStats(prevHeadSlot)
		}

		// log missed slots
		for slot := prevHeadSlot + 1; slot < headSlot; slot++ {
			log := m.log.WithFields(logrus.Fields{
				"headSlot":     headSlot,
				"prevHeadSlot": prevHeadSlot,
				"missedSlot":   slot,
			})
			log.Warnf("missed slot: %d", slot)

			go func(slot uint64) {
				builderPubkey, err := m.datastore.GetDeliveredPayload(context.Background(), slot)
				if err != nil || builderPubkey == "" {
					return
				}

				log.WithField("builderPubkey", builderPubkey).Warn("delivered payload for missed slot")

				if err := m.datastore.SetDemotedBuilderPubkey(context.Background(), builderPubkey); err != nil {
					log.WithError(err).Errorf("could not demote builder %s", builderPubkey)
				}
			}(slot)

			go func(slot uint64) {
				keysToExpire, ok := m.slotKeysToExpire.Load(slot)
				if !ok {
					return
				}
				pipeline := m.datastore.TxPipeline()
				keysToExpire.Range(func(key string, value bool) bool {
					pipeline.Expire(context.Background(), key, 30*time.Second)
					return true
				})

				_, err := pipeline.Exec(context.Background())
				if err != nil {
					log.WithError(err).Error("Could not expire old keys")
					return
				}
				log.Infof("expired %v keys for slot %v", keysToExpire.Size(), slot)
				m.slotKeysToExpire.Delete(slot)
			}(slot)
		}
	} else {
		go func(slot uint64) {
			keysToExpire, ok := m.slotKeysToExpire.Load(slot)
			if !ok {
				return
			}
			pipeline := m.datastore.TxPipeline()
			keysToExpire.Range(func(key string, value bool) bool {
				pipeline.Expire(context.Background(), key, 30*time.Second)
				return true
			})

			_, err := pipeline.Exec(context.Background())
			if err != nil {
				m.log.WithError(err).Error("Could not expire old keys")
				return
			}
			m.log.Infof("expired %v keys for slot %v", keysToExpire.Size(), slot)
			m.slotKeysToExpire.Delete(slot)
		}(headSlot - 1)
	}

	// keep track of slot start time
	m.slotStartTimes.Store(headSlot, newSlotTime)
	previousSlot := headSlot - 1
	penultimateSlot := headSlot - 2
	penultimateTime, found := m.getPayloadRequests.Load(penultimateSlot)
	previousSlotStartTime, _ := m.slotStartTimes.Load(previousSlot)

	// log slot start time and if getPayload request was received
	if found {
		getPayloadTimeDiff := previousSlotStartTime.Sub(penultimateTime.timestamp).Seconds()
		m.log.Infof("slot %v started at %v, get payload request for slot %v was received at %v, %v seconds from the slot start time",
			previousSlot, previousSlotStartTime.String(), penultimateSlot, penultimateTime.timestamp, getPayloadTimeDiff)
	} else {
		m.log.Infof("slot %v started at %v, no get payload request was received for slot %v",
			previousSlot, previousSlotStartTime.String(), penultimateSlot)
	}

	// log the best builder block for slot after 11 seconds (about 1 second before next slot)
	m.bestBlockLoggingTimer = time.AfterFunc(bestBlockLoggingTimerInterval, func() {
		if bestBlockBidForSlot, found := m.bestBlockBidForSlot.Load(headSlot); found {
			if m.stats.NodeID == "" {
				return
			}
			stat := bestBuilderBlockForSlotStat{
				Slot:                 bestBlockBidForSlot.Slot,
				SlotStartTime:        newSlotTime.String(),
				BlockHash:            bestBlockBidForSlot.BlockHash.String(),
				ParentHash:           bestBlockBidForSlot.ParentHash.String(),
				ProposerPubkey:       bestBlockBidForSlot.ProposerPubkey.String(),
				Value:                bestBlockBidForSlot.Value.String(),
				EthValue:             common.WeiToEth(bestBlockBidForSlot.Value.String()),
				BestBlocksFromRelays: m.getBestBlocksFromExternalRelays(bestBlockBidForSlot.Slot, bestBlockBidForSlot.ParentHash.String(), bestBlockBidForSlot.ProposerPubkey.String()),
			}

			record := statistics.Record{
				Data: &stat,
				Type: "StatsBestBuilderBlockForSlot",
			}
			m.stats.LogToFluentD(record, time.Now(), statsNameBlockComparison)

			return
		}
		m.log.Infof("no builder blocks saved for slot %v", headSlot)
	})
	m.bestBlockLoggingTimer.Reset(bestBlockLoggingTimerInterval)

	m.log.Infof("fetching block data for slot %v", headSlot)
	go func(slot uint64) {
		if block := m.fetchBlockData(fmt.Sprintf("%v", slot), false); block != nil {
			m.recentBlocksLock.Lock()
			m.recentBlocks = append([]blockStat{*block}, m.recentBlocks...)
			if len(m.recentBlocks) > 25 {
				m.recentBlocks = m.recentBlocks[:24]
			}
			m.recentBlocksLock.Unlock()
		}
	}(headSlot)

	if headSlot%slotCleanupInterval == 0 {
		m.datastore.CleanupOldBidsAndBlocks(headSlot)
		m.cleanupOldSlots(headSlot)
	}
	// Regularly update proposer duties in the background
	go m.updateProposerDuties(headSlot)
}

func (m *BoostService) processNewPayloadAttributes(event beaconclient.PayloadAttributesData) {
	m.log.WithField("event", event).Info("new payload attributes")
	if event.ProposalSlot == 0 {
		m.log.Info("empty payload attributes, skipping")
		return
	}
	m.nextSlotWithdrawalsLock.Lock()
	m.nextSlotWithdrawals = event.PayloadAttributes.Withdrawals
	nextRoot, err := ComputeWithdrawalsRoot(m.nextSlotWithdrawals)
	if err != nil {
		m.log.WithError(err).Warn("could not compute withdrawals root")
		return
	}
	m.nextSlotWithdrawalsRoot = nextRoot
	m.nextSlotWithdrawalsLock.Unlock()

	m.expectedPayloadDataLock.Lock()
	m.expectedBlockNumber = event.ParentBlockNumber + 1
	m.expectedPrevRandao = randaoHelper{prevRandao: event.PayloadAttributes.PrevRandao, slot: event.ProposalSlot}
	m.expectedPayloadDataLock.Unlock()

	if m.enableBidSaveCancellation {
		m.newPayloadAttributesSlotChannel <- event.ProposalSlot
	}

	go func(slot uint64) {
		keysToExpire, ok := m.slotKeysToExpire.Load(slot)
		if !ok {
			return
		}
		pipeline := m.datastore.TxPipeline()
		keysToExpire.Range(func(key string, value bool) bool {
			pipeline.Expire(context.Background(), key, 30*time.Second)
			return true
		})

		_, err := pipeline.Exec(context.Background())
		if err != nil {
			m.log.WithError(err).Error("Could not expire old keys")
			return
		}
		m.log.Infof("expired %v keys for slot %v", keysToExpire.Size(), slot)
		m.slotKeysToExpire.Delete(slot)
	}(event.ProposalSlot - 1)
}

// clean up old slot times
func (m *BoostService) cleanupOldSlots(headSlot uint64) {
	for _, keySlot := range m.slotStartTimes.Keys() {
		if keySlot < headSlot-slotCleanupInterval {
			m.slotStartTimes.Delete(keySlot)
		}
	}

	for _, keySlot := range m.getPayloadRequests.Keys() {
		if keySlot < headSlot-slotCleanupInterval {
			m.getPayloadRequests.Delete(keySlot)
		}
	}

	for _, keySlot := range m.getHeaderRequests.Keys() {
		if keySlot < headSlot-slotCleanupInterval {
			m.getHeaderRequests.Delete(keySlot)
		}
	}

	for _, keySlot := range m.bestBlockBidForSlot.Keys() {
		if keySlot < headSlot-slotCleanupInterval {
			m.bestBlockBidForSlot.Delete(keySlot)
		}
	}

	for _, keySlot := range m.bestBloxrouteBlockBidForSlot.Keys() {
		if keySlot < headSlot-slotCleanupInterval {
			m.bestBloxrouteBlockBidForSlot.Delete(keySlot)
		}
	}

	for _, keySlot := range m.builderContextsForSlot.Keys() {
		if keySlot < headSlot-slotCleanupInterval {
			m.builderContextsForSlot.Delete(keySlot)
		}
	}
}

// logSlotStats logs the getPayload, getHeader and proposerLastRegistered stats for the previous slot
func (m *BoostService) logSlotStats(prevHeadSlot uint64) {
	slotStatsRecord := &slotStatsRecord{
		Slot:                   prevHeadSlot,
		LastRegisteredProposer: m.getLastRegisteredProposer(prevHeadSlot),
	}

	req, ok := m.getPayloadRequests.Load(prevHeadSlot)
	if ok {
		slotStatsRecord.GetPayloadRequest = &requestStatsRecord{
			Timestamp: req.timestamp.UnixNano(),
		}
	}

	// if both GetPayloadRequest and LastRegisteredProposer are nil, don't log
	if slotStatsRecord.LastRegisteredProposer == nil && slotStatsRecord.GetPayloadRequest == nil {
		return
	}

	getHeaderRequests, _ := m.getHeaderRequests.Load(prevHeadSlot)

	for _, req := range getHeaderRequests {
		// getPayload IP is equal to the getHeader request IP
		// OR
		// validator registration's IP is equal to the getHeader request IP
		if (slotStatsRecord.GetPayloadRequest != nil) ||
			(slotStatsRecord.LastRegisteredProposer != nil) {

			slotStatsRecord.GetHeaderRequest = &requestStatsRecord{
				Timestamp: req.timestamp.UnixNano(),
			}

			slotStatsRecord.BlockHash = req.blockHash

			// either slotStatsRecord.GetPayloadRequest or slotStatsRecord.LastRegisteredProposer
			// might be nil
			m.stats.LogToFluentD(statistics.Record{
				Data: slotStatsRecord,
				Type: "StatsSlot",
			}, time.Now(), statsNameSlot)

			return
		}
	}
}

func (m *BoostService) getLastRegisteredProposer(prevHeadSlot uint64) *validatorStatsRecord {
	// the IP and timestamp when the proposer last registered
	registeredValidator, _ := m.proposerDutiesMap.Load(prevHeadSlot)

	if registeredValidator == nil {
		m.log.Warnf("could not find registered validator for slot %d in proposer duties", prevHeadSlot)
		return nil
	}

	activeValidator, err := m.datastore.GetActiveValidator(context.Background(), registeredValidator.Pubkey.String())
	if err != nil {
		m.log.WithError(err).Errorf("could not get active validator from redis with pubkey %s", registeredValidator.Pubkey.PubkeyHex())
		return nil
	}

	return &validatorStatsRecord{
		requestStatsRecord: requestStatsRecord{
			Timestamp: activeValidator.LastRegistered,
		},
		ProposerPublicKey: registeredValidator.Pubkey.PubkeyHex().String(),
	}
}

func (m *BoostService) getBestBlocksFromExternalRelays(slot uint64, parentHash, pubKey string) map[string]BestBlockFromRelay {
	bestBlocksFromRelay := map[string]BestBlockFromRelay{}

	baseURL := fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", slot, parentHash, pubKey)

	wg := &sync.WaitGroup{}
	wg.Add(len(m.externalRelaysForComparison))

	for _, relayURL := range m.externalRelaysForComparison {
		go func(wg *sync.WaitGroup, relayURL string) {
			defer wg.Done()

			// we want to keep empty data even for failed request
			bestBlocksFromRelay[relayURL] = BestBlockFromRelay{}

			url := fmt.Sprintf("%s%s", relayURL, baseURL)
			resp, err := common.MakeRequest(context.Background(), m.httpClient, http.MethodGet, url, nil)
			if err != nil {
				m.log.WithError(err).Errorf("could not fetch best block from relay: %s", relayURL)
				return
			}

			bestBlockRelayResponse := types.GetHeaderResponse{}
			err = json.NewDecoder(resp.Body).Decode(&bestBlockRelayResponse)
			if err != nil {
				m.log.WithError(err).Errorf("could not decode best block response from relay: %s", relayURL)
				return
			}

			bestBlocksFromRelay[relayURL] = BestBlockFromRelay{
				BlockHash:     bestBlockRelayResponse.Data.Message.Header.BlockHash.String(),
				ParentHash:    bestBlockRelayResponse.Data.Message.Header.ParentHash.String(),
				BlockValueETH: common.WeiToEth(bestBlockRelayResponse.Data.Message.Value.String()),
				BlockValueWEI: bestBlockRelayResponse.Data.Message.Value.String(),
			}

		}(wg, relayURL)
	}
	wg.Wait()

	return bestBlocksFromRelay
}

func (m *BoostService) fetchBlockData(slotNumber string, fetchingMissingBlock bool) *blockStat {
	time.Sleep(3 * time.Second)
	beaconBlock := beaconBlockResponse{}
	beaconBlockFound := false

	for _, beaconURL := range m.beaconClient.GetBeaconNodeURLs() {
		url := fmt.Sprintf("%v/eth/v2/beacon/blocks/%v", beaconURL, slotNumber)
		if code, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet, url, "mev-boost-relay", nil, &beaconBlock); err != nil {
			m.log.WithError(err).WithField("response_code", code).Warnf("could not fetch block data for slot %s, beacon node: %s", slotNumber, beaconURL)
			continue
		}
		beaconBlockFound = true
		break
	}
	if !beaconBlockFound {
		m.log.Warnf("could not fetch block data for slot %s from any beacon nodes, unable to log stats", slotNumber)
		return nil
	}

	headerFound := false
	payloadFound := false
	bestHeaderPayload := builderBestHeaderPayload{
		Header:  capella.ExecutionPayloadHeader{},
		Payload: capella.ExecutionPayload{},
		Profit:  nil,
	}

	slot, err := strconv.ParseUint(slotNumber, 10, 64)
	if err != nil {
		m.log.WithError(err).Warnf("could not parse uint64 from slot number %s", slotNumber)
	}

	proposerData, found := m.allProposerDuties.Load(slot)
	if !found {
		m.log.WithError(err).Warnf("could not fetch bloxroute block data for slot %v, proposer public key not found", slot)
	}

	_, headerResponse, _, err := m.datastore.GetGetHeaderResponse(context.Background(), slot, beaconBlock.Data.Message.Body.ExecutionPayload.ParentHash.String(), proposerData.Pubkey)
	if err != nil || headerResponse == nil || headerResponse.Capella == nil || headerResponse.Capella.Capella == nil {
		m.log.WithError(err).Warnf("could not fetch bloxroute block data for slot %v: %v", slot, err)
	} else {
		bestHeaderPayload.Header = *headerResponse.Capella.Capella.Message.Header
		bestHeaderPayload.Profit = headerResponse.Capella.Capella.Message.Value.ToBig()
		headerFound = true
	}

	if headerFound {
		payloadResponse, err := m.getGetPayloadResponse(slot, bestHeaderPayload.Header.BlockHash.String())
		if err != nil || payloadResponse == nil || payloadResponse.Capella == nil || payloadResponse.Capella.Capella == nil {
			m.log.WithError(err).Warnf("could not fetch bloxroute block data for slot %s, best payload not found", slotNumber)
		} else {
			bestHeaderPayload.Payload = *payloadResponse.Capella.Capella
			payloadFound = true
		}
	}

	profit, err := m.getCoinbaseBalanceChange(beaconBlock.Data.Message.Body.ExecutionPayload.FeeRecipient.String(), int(beaconBlock.Data.Message.Body.ExecutionPayload.BlockNumber))
	if err != nil {
		m.log.WithError(err).Warn("could not fetch block profit")
		return nil
	}
	profit = m.adjustBlockProfit(beaconBlock, profit)

	logBeaconData := beaconBlock.Data.Message
	// since this might have a lot of transactions, we don't want to log it to save space
	logBeaconData.Body.ExecutionPayload.Transactions = []hexutil.Bytes{}

	stat := blockStat{
		ProposedBlockMessage: logBeaconData,
		WasBXProposed:        strings.EqualFold(string(logBeaconData.Body.ExecutionPayload.ExtraData), "Powered by bloXroute"),
		ConfirmedBlockProfit: profit,
		BXBlockData:          builderBestHeaderPayload{Profit: big.NewInt(0)},
	}
	if headerFound && payloadFound {
		stat.BXBlockData = bestHeaderPayload
		stat.BXBlockData.Payload.Transactions = []bellatrix.Transaction{}
	}

	if m.stats.NodeID != "" {
		record := statistics.Record{
			Data: &stat,
			Type: "StatsBlockComparison",
		}
		m.stats.LogToFluentD(record, time.Now(), statsNameBlockComparison)
	}

	m.logBlockComparisonWithProvidedHeaders(slotNumber, stat)

	if !fetchingMissingBlock {
		slot, ok := math.ParseUint64(slotNumber)
		if !ok {
			m.log.Errorf("could not set latest fetched slot number, %s is not an uint64", slotNumber)
			return &stat
		}
		previousLatestFetchedSlot := m.latestFetchedSlotNumber
		m.latestFetchedSlotNumber = slot
		missingSlots := m.latestFetchedSlotNumber - previousLatestFetchedSlot - 1
		if previousLatestFetchedSlot != 0 && missingSlots > 0 {
			for slotToFetch := previousLatestFetchedSlot + 1; slotToFetch < m.latestFetchedSlotNumber; slotToFetch++ {
				m.log.Infof("fetching missed block data for slot %v", slotToFetch)
				m.fetchBlockData(strconv.FormatUint(slotToFetch, 10), true)
			}
		}
	}

	return &stat
}

func (m *BoostService) getGetPayloadResponse(slot uint64, blockHash string) (*common.GetPayloadResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDatabaseRequestTimeout)
	defer cancel()

	return m.datastore.GetGetPayloadResponse(ctx, slot, blockHash)
}

func (m *BoostService) logBlockComparisonWithProvidedHeaders(slotNumber string, stat blockStat) {
	slotInt, err := strconv.ParseInt(slotNumber, 10, 64)
	if err != nil {
		m.log.WithError(err).Error("slot number is not an int64")
		return
	}

	bestProvidedHeader := new(common.GetHeaderResponse)
	providedHeaders, ok := m.providedHeaders.Load(slotInt)
	if !ok || len(providedHeaders.Headers) == 0 {
		m.log.WithField("slot", slotNumber).Info("no provided headers for slot")
	} else {
		m.providedHeaders.Delete(slotInt)
		for _, header := range providedHeaders.Headers {
			if bestProvidedHeader.Capella == nil || bestProvidedHeader.Capella.Capella == nil || bestProvidedHeader.Capella.Capella.Message.Value.ToBig().Cmp(header.Capella.Capella.Message.Value.ToBig()) < 1 {
				bestProvidedHeader = header
			}
		}
	}
	_, payloadProvided := m.providedPayload.Load(slotInt)
	if !payloadProvided {
		m.log.WithField("slot", slotNumber).Info("no provided payload for slot")
	} else {
		m.providedPayload.Delete(slotInt)
	}

	logBeaconData := stat.ProposedBlockMessage
	logBeaconData.Body.ExecutionPayload.Transactions = []hexutil.Bytes{}
	logBXBlockData := stat.BXBlockData
	logBXBlockData.Payload.Transactions = []bellatrix.Transaction{}
	parentHash := stat.BXBlockData.Payload.ParentHash.String()

	_, slotBestHeader, _, err := m.datastore.GetGetHeaderResponse(context.Background(), uint64(slotInt), parentHash, providedHeaders.ProposerPublicKey)
	if slotBestHeader == nil || err != nil {
		m.log.WithField("slot", slotNumber).Warn(fmt.Sprintf("no best header in datastore for slot: %v, parent hash: %v, proposer public key: %v, error: %v", uint64(slotInt), parentHash, providedHeaders.ProposerPublicKey, err))
	}

	storedValidatorRegistration, err := m.datastore.GetValidatorRegistration(context.Background(),
		types.PubkeyHex(providedHeaders.ProposerPublicKey))
	proposerRegistered := storedValidatorRegistration != nil && err == nil

	loggedStats := blockStat{
		ProposedBlockMessage: logBeaconData,
		WasBXProposed:        stat.WasBXProposed,
		ConfirmedBlockProfit: stat.ConfirmedBlockProfit,
		BXBlockData:          logBXBlockData,
		ProvidedHeaders:      providedHeaders.Headers,
		BestProvidedHeader:   bestProvidedHeader,
		BestHeader:           slotBestHeader,
		HeaderProvided:       len(providedHeaders.Headers) > 0,
		PayloadProvided:      payloadProvided,
		ProposerRegistered:   proposerRegistered,
	}

	if m.stats.NodeID != "" {
		record := statistics.Record{
			Data: loggedStats,
			Type: "StatsBlockComparisonWithProvidedHeaders",
		}
		m.stats.LogToFluentD(record, time.Now(), statsNameBlockComparisonWithProvidedHeaders)
	}

}

func (m *BoostService) txIsFromFeeRecipient(transaction hexutil.Bytes, feeRecipient types.Address) (*ethTypes.Transaction, bool) {
	tx := &ethTypes.Transaction{}
	err := tx.UnmarshalBinary(transaction[:])
	if err != nil {
		return nil, false
	}
	sender, err := ethTypes.LatestSignerForChainID(tx.ChainId()).Sender(tx)
	if err != nil {
		return nil, false
	}
	return tx, strings.EqualFold(sender.String(), feeRecipient.String())
}

func (m *BoostService) adjustBlockProfit(beaconBlock beaconBlockResponse, profit *big.Int) *big.Int {
	slotNumber := beaconBlock.Data.Message.Slot
	numberOfTxs := len(beaconBlock.Data.Message.Body.ExecutionPayload.Transactions)
	feeRecipient := beaconBlock.Data.Message.Body.ExecutionPayload.FeeRecipient
	blockNumber := beaconBlock.Data.Message.Body.ExecutionPayload.BlockNumber
	adjustedBlockProfit := new(big.Int).Set(profit)

	// check the last transaction in the block to see if it is a likely payout
	if numberOfTxs > 0 {
		rawTx := beaconBlock.Data.Message.Body.ExecutionPayload.Transactions[numberOfTxs-1]
		m.log.Infof("last tx in block: %s, slot: %v, blockNumber: %v", rawTx.String(), slotNumber, blockNumber)
		lastTx, ok := m.txIsFromFeeRecipient(rawTx, feeRecipient)
		if lastTx == nil {
			m.log.Errorf("could not parse last tx, slot: %v, blockNumber: %v", slotNumber, blockNumber)
		} else if lastTx.Value().Cmp(big.NewInt(0)) == 0 {
			m.log.Infof("last tx value is 0, slot: %v, blockNumber: %v", slotNumber, blockNumber)
		} else if !ok {
			m.log.Infof("last tx is not from fee recipient, slot: %v, blockNumber: %v", slotNumber, blockNumber)
		} else {
			// if last tx is a likely payout, set block profit to the ETH value of that last tx
			adjustedBlockProfit = lastTx.Value()
			m.log.Infof("most likely last tx is payout, slot: %v, blockNumber: %v, value: %v", slotNumber, blockNumber, common.WeiToEth(lastTx.Value().String()))
		}
	}

	// look for profit adjustment transactions, ignoring the last transaction in the block
	for i := 0; i < numberOfTxs-1; i++ {
		rawTx := beaconBlock.Data.Message.Body.ExecutionPayload.Transactions[i]
		if tx, ok := m.txIsFromFeeRecipient(rawTx, feeRecipient); ok {
			adjustedBlockProfit = adjustedBlockProfit.Add(adjustedBlockProfit, tx.Cost())
			m.log.Infof("coinbase profit adjusted, slot: %v, blockNumber: %v, tx hash: %s, profit adjustment: +%v, new profit: %v", slotNumber, blockNumber, tx.Hash().String(), common.WeiToEth(tx.Cost().String()), common.WeiToEth(profit.String()))
		}
	}

	return adjustedBlockProfit
}

// TODO: Update this method to check transactions within the block to check for value
// transferred to the proposer
func (m *BoostService) getCoinbaseBalanceChange(address string, blockNumber int) (*big.Int, error) {
	msgBalanceBefore := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBalance","params": ["%v", "0x%x"],"id":1}"`, address, blockNumber-1))
	msgBalanceAfter := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBalance","params": ["%v", "0x%x"],"id":1}"`, address, blockNumber))

	balanceBefore, err := fetchBalance(m.executionNode.String(), msgBalanceBefore)
	if err != nil {
		return big.NewInt(0), err
	}
	balanceAfter, err := fetchBalance(m.executionNode.String(), msgBalanceAfter)
	if err != nil {
		return big.NewInt(0), err
	}

	if len(balanceBefore.Result) < 2 || len(balanceAfter.Result) < 2 {
		return big.NewInt(0), errors.New("wrong lengths")
	}

	balanceBeforeInt := new(big.Int)
	balanceBeforeInt.SetString(balanceBefore.Result[2:], 16)
	balanceAfterInt := new(big.Int)
	balanceAfterInt.SetString(balanceAfter.Result[2:], 16)

	return balanceBeforeInt.Sub(balanceAfterInt, balanceBeforeInt), nil
}

func fetchBalance(url string, msg []byte) (*rpcResponse, error) {
	balance := rpcResponse{}
	client := http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept-Encoding", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(res.Body).Decode(&balance); err != nil {
		return nil, err
	}
	return &balance, err
}

func (m *BoostService) FetchPreviousData(slot uint64) error {
	log := m.log.WithField("headSlot", slot)
	beaconBlock, err := m.beaconClient.GetSlotBlock(slot)
	if err != nil {
		log.WithError(err).Error("could not fetch prev block data")
		return err
	}

	m.expectedPayloadDataLock.Lock()
	defer m.expectedPayloadDataLock.Unlock()
	prevRandao, err := calculateNextPrevRandao(beaconBlock.Body.RandaoReveal, []byte(beaconBlock.Body.ExecutionPayload.Random.String()))
	if err != nil {
		log.WithFields(logrus.Fields{
			"block":  beaconBlock,
			"reveal": beaconBlock.Body.RandaoReveal,
			"random": beaconBlock.Body.ExecutionPayload.Random.String(),
		}).WithError(err).Error("could not fetch prev block data")
		return err
	}

	m.expectedPrevRandao = randaoHelper{
		slot:       slot + 1,
		prevRandao: fmt.Sprintf("%#x", prevRandao),
	}

	m.expectedBlockNumber = beaconBlock.Body.ExecutionPayload.BlockNumber + 1
	log.WithFields(logrus.Fields{
		"expectedPrevRandao":  m.expectedPrevRandao,
		"expectedBlockNumber": m.expectedBlockNumber,
	}).Info("fetched prev block data")
	return nil
}

func calculateNextPrevRandao(reveal string, prevRandao []byte) ([]byte, error) {
	decodedReveal, err := hexutil.Decode(reveal)
	if err != nil {
		return nil, err
	}
	r := hash(decodedReveal)
	n := ethCommon.HexToHash(string(prevRandao))
	for i, b := range r {
		n[i] ^= b
	}

	return n.Bytes(), nil
}

var sha256Pool = sync.Pool{New: func() interface{} {
	return sha256.New()
}}

type Hash interface {
	// Write (via the embedded io.Writer interface) adds more data to the running hash.
	// It never returns an error.
	io.Writer

	// Sum appends the current hash to b and returns the resulting slice.
	// It does not change the underlying hash state.
	Sum(b []byte) []byte

	// Reset resets the Hash to its initial state.
	Reset()

	// Size returns the number of bytes Sum will return.
	Size() int

	// BlockSize returns the hash's underlying block size.
	// The Write method must be able to accept any amount
	// of data, but it may operate more efficiently if all writes
	// are a multiple of the block size.
	BlockSize() int
}

func hash(data []byte) [32]byte {
	h, ok := sha256Pool.Get().(Hash)
	if !ok {
		h = sha256.New()
	}
	defer sha256Pool.Put(h)
	h.Reset()

	var b [32]byte

	// The hash interface never returns an error, for that reason
	// we are not handling the error below. For reference, it is
	// stated here https://golang.org/pkg/hash/#Hash

	// #nosec G104
	h.Write(data)
	h.Sum(b[:0])

	return b
}
