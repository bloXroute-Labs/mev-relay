package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/math"

	"github.com/bloXroute-Labs/mev-relay/beaconclient"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/go-boost-utils/types"
)

type builderBestHeaderPayload struct {
	Header  types.ExecutionPayloadHeader `json:"header"`
	Payload types.ExecutionPayload       `json:"payload"`
	Profit  *big.Int                     `json:"profit"`
}

const slotCleanupInterval = 5

// sendNextSlotProposerDuties sends the next slot's proposer info to cloud services
func (m *RelayService) sendNextSlotProposerDuties(currentSlot uint64) {
	if !m.sendSlotProposerDuties {
		return
	}
	nextSlot := currentSlot + 1
	proposerDutiesForNextSlot, ok := m.allProposerDuties[nextSlot]
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
func (m *RelayService) SubscribeToBlocks() {
	go func() {
		c := make(chan beaconclient.HeadEventData)
		m.beaconClient.SubscribeToHeadEvents(c)
		for {
			headEvent := <-c
			isEmptyHeadEvent := headEvent.Slot == 0 && headEvent.Block == "" && headEvent.State == ""

			// beacon node will send an empty event if proposer slot event is missed
			if isEmptyHeadEvent && !m.emptyBeaconHeadEventReceived {
				m.emptyBeaconHeadEventTimer.Reset(emptyBeaconHeadEventTimerInterval)
				m.emptyBeaconHeadEventReceived = true

				m.latestSlotBlockReceived++
				m.log.Warnf("empty beacon head event received, assuming slot %v", m.latestSlotBlockReceived)
				m.sendNextSlotProposerDuties(m.latestSlotBlockReceived)
				m.processNewSlot(m.latestSlotBlockReceived)
			}

			if !isEmptyHeadEvent && headEvent.Slot > m.latestSlotBlockReceived {
				m.latestSlotBlockReceived = headEvent.Slot
				m.sendNextSlotProposerDuties(m.latestSlotBlockReceived)
				m.processNewSlot(m.latestSlotBlockReceived)
			}
		}
	}()
}

func (m *RelayService) processNewSlot(slot uint64) {
	if slot%slotCleanupInterval == 0 {
		m.datastore.CleanupOldBidsAndBlocks(slot)
	}
	// Regularly update proposer duties in the background
	go m.updateProposerDuties(slot)
}

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
	ProvidedHeaders      []*types.GetHeaderResponse
	BestProvidedHeader   *types.GetHeaderResponse
	BestHeader           *types.GetHeaderResponse
	HeaderProvided       bool
	PayloadProvided      bool
	ProposerRegistered   bool
}

type beaconBlock struct {
	ExecutionPayload types.ExecutionPayload `json:"execution_payload"`
}

func (m *RelayService) fetchBlockData(slotNumber string, fetchingMissingBlock bool) *blockStat {
	time.Sleep(3 * time.Second)
	beaconBlock := beaconBlockResponse{}
	beaconBlockFound := false

	for _, beaconURL := range m.beaconClient.GetBeaconNodeURLs() {
		url := fmt.Sprintf("%v/eth/v2/beacon/blocks/%v", beaconURL, slotNumber)
		if _, err := SendHTTPRequest(context.Background(), m.httpClient, http.MethodGet, url, "mev-boost-relay", nil, &beaconBlock); err != nil {
			m.log.WithError(err).Warnf("could not fetch block data for slot %s, beacon node: %s", slotNumber, beaconURL)
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
		Header:  types.ExecutionPayloadHeader{},
		Payload: types.ExecutionPayload{},
		Profit:  nil,
	}

	slot, err := strconv.ParseUint(slotNumber, 10, 64)
	if err != nil {
		m.log.WithError(err).Warnf("could not parse uint64 from slot number %s", slotNumber)
	}

	proposerData, found := m.allProposerDuties[slot]
	if !found {
		m.log.WithError(err).Warnf("could not fetch bloxroute block data for slot %v, proposer public key not found", slot)
	}

	headerResponse, err := m.datastore.GetGetHeaderResponse(slot, beaconBlock.Data.Message.Body.ExecutionPayload.ParentHash.String(), proposerData.Pubkey)
	if err != nil || headerResponse == nil || headerResponse.Data == nil {
		m.log.WithError(err).Warnf("could not fetch bloxroute block data for slot %v, best header not found", slot)
	} else {
		bestHeaderPayload.Header = *headerResponse.Data.Message.Header
		bestHeaderPayload.Profit = headerResponse.Data.Message.Value.BigInt()
		headerFound = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), databaseRequestTimeout)
	defer cancel()

	if headerFound {
		payloadResponse, err := m.datastore.GetGetPayloadResponse(ctx, slot, bestHeaderPayload.Header.BlockHash.String())
		if err != nil || payloadResponse == nil || payloadResponse.Data == nil {
			m.log.WithError(err).Warnf("could not fetch bloxroute block data for slot %s, best payload not found", slotNumber)
		} else {
			bestHeaderPayload.Payload = *payloadResponse.Data
			payloadFound = true
		}
	}

	profit, err := m.getCoinbaseBalanceChange(beaconBlock.Data.Message.Body.ExecutionPayload.FeeRecipient.String(), int(beaconBlock.Data.Message.Body.ExecutionPayload.BlockNumber))
	if err != nil {
		m.log.WithError(err).Warn("could not fetch block profit")
		return nil
	}
	profit = m.adjustBlockProfit(beaconBlock, profit)

	stat := blockStat{
		ProposedBlockMessage: beaconBlock.Data.Message,
		WasBXProposed:        strings.EqualFold(string(beaconBlock.Data.Message.Body.ExecutionPayload.ExtraData), "Powered by bloXroute"),
		ConfirmedBlockProfit: profit,
		BXBlockData:          builderBestHeaderPayload{Profit: big.NewInt(0)},
	}
	if headerFound && payloadFound {
		stat.BXBlockData = bestHeaderPayload
	}

	record := map[string]interface{}{
		"data": &stat,
		"type": "StatsBlockComparison",
	}
	m.log.Info(record, time.Now(), "stats.block_comparison")

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

func (m *RelayService) logBlockComparisonWithProvidedHeaders(slotNumber string, stat blockStat) error {
	slotInt, err := strconv.ParseInt(slotNumber, 10, 64)
	if err != nil {
		m.log.WithError(err).Error("slot number is not an int64")
		return err
	}

	bestProvidedHeader := new(types.GetHeaderResponse)
	providedHeaders, ok := m.providedHeaders.Get(slotInt)
	if !ok || len(providedHeaders.Headers) == 0 {
		m.log.WithField("slot", slotNumber).Info("no provided headers for slot")
	} else {
		m.providedHeaders.Del(slotInt)
		for _, header := range providedHeaders.Headers {
			if bestProvidedHeader.Data == nil || bestProvidedHeader.Data.Message.Value.BigInt().Cmp(header.Data.Message.Value.BigInt()) < 1 {
				bestProvidedHeader = header
			}
		}
	}
	_, payloadProvided := m.providedPayload.Get(slotInt)
	if !payloadProvided {
		m.log.WithField("slot", slotNumber).Info("no provided payload for slot")
	} else {
		m.providedPayload.Del(slotInt)
	}

	logBeaconData := stat.ProposedBlockMessage
	logBeaconData.Body.ExecutionPayload.Transactions = []hexutil.Bytes{}
	logBXBlockData := stat.BXBlockData
	logBXBlockData.Payload.Transactions = []hexutil.Bytes{}
	parentHash := stat.BXBlockData.Payload.ParentHash.String()

	slotBestHeader, err := m.datastore.GetGetHeaderResponse(uint64(slotInt), parentHash, providedHeaders.ProposerPublicKey)
	if slotBestHeader == nil || err != nil {
		m.log.WithField("slot", slotNumber).Warn(fmt.Sprintf("no best header in datastore for slot: %v, parent hash: %v, proposer public key: %v, error: %v", uint64(slotInt), parentHash, providedHeaders.ProposerPublicKey, err))
	}

	storedValidatorRegistration, err := m.datastore.GetValidatorRegistration(types.PubkeyHex(providedHeaders.ProposerPublicKey))
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

	record := map[string]interface{}{
		"data": loggedStats,
		"type": "StatsBlockComparisonWithProvidedHeaders",
	}
	m.log.Info(record, time.Now(), "stats.block_comparison_with_provided_headers")
	return nil
}

func (m *RelayService) txIsFromFeeRecipient(transaction hexutil.Bytes, feeRecipient types.Address) (*ethTypes.Transaction, bool) {
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

func (m *RelayService) adjustBlockProfit(beaconBlock beaconBlockResponse, profit *big.Int) *big.Int {
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
			m.log.Infof("most likely last tx is payout, slot: %v, blockNumber: %v, value: %v", slotNumber, blockNumber, weiToEth(lastTx.Value().String()))
		}
	}

	// look for profit adjustment transactions, ignoring the last transaction in the block
	for i := 0; i < numberOfTxs-1; i++ {
		rawTx := beaconBlock.Data.Message.Body.ExecutionPayload.Transactions[i]
		if tx, ok := m.txIsFromFeeRecipient(rawTx, feeRecipient); ok {
			adjustedBlockProfit = adjustedBlockProfit.Add(adjustedBlockProfit, tx.Cost())
			m.log.Infof("coinbase profit adjusted, slot: %v, blockNumber: %v, tx hash: %s, profit adjustment: +%v, new profit: %v", slotNumber, blockNumber, tx.Hash().String(), weiToEth(tx.Cost().String()), weiToEth(profit.String()))
		}
	}

	return adjustedBlockProfit
}

// TODO: Update this method to check transactions within the block to check for value
// transferred to the proposer
func (m *RelayService) getCoinbaseBalanceChange(address string, blockNumber int) (*big.Int, error) {
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
