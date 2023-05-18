// Package beaconclient provides a beacon-node client
package beaconclient

import (
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/sirupsen/logrus"
	uberatomic "go.uber.org/atomic"
)

var (
	ErrBeaconNodeSyncing      = errors.New("beacon node is syncing or unavailable")
	ErrBeaconNodesUnavailable = errors.New("all beacon nodes responded with error")
)

// IMultiBeaconClient is the interface for the MultiBeaconClient, which can manage several beacon client instances under the hood
type IMultiBeaconClient interface {
	BestSyncStatus() (*SyncStatusPayloadData, error)
	SubscribeToHeadEvents(slotC chan HeadEventData)
	SubscribeToPayloadAttributes(payloadAttributesChan chan PayloadAttributesData)

	// FetchValidators returns all active and pending validators from the beacon node
	FetchValidators(headSlot uint64) (map[types.PubkeyHex]ValidatorResponseEntry, error)
	GetProposerDuties(epoch uint64) (*ProposerDutiesResponse, error)
	PublishBlock(block *capella.SignedBeaconBlock) (code int, err error)
}

// IBeaconInstance is the interface for a single beacon client instance
type IBeaconInstance interface {
	SyncStatus() (*SyncStatusPayloadData, error)
	CurrentSlot() (uint64, error)
	SubscribeToHeadEvents(slotC chan HeadEventData)
	SubscribeToPayloadAttributes(payloadAttributesChan chan PayloadAttributesData)
	FetchValidators(headSlot uint64) (map[types.PubkeyHex]ValidatorResponseEntry, error)
	GetProposerDuties(epoch uint64) (*ProposerDutiesResponse, error)
	GetURI() string
	GetSlotBlock(slot uint64) (res *common.BeaconBlockMessage, err error)

	GetWithdrawals(slot uint64) (withdrawalsResp *GetWithdrawalsResponse, err error)
	PublishBlock(block *capella.SignedBeaconBlock) (code int, err error)
}

type MultiBeaconClient struct {
	log             *logrus.Entry
	bestBeaconIndex uberatomic.Int64
	beaconInstances []IBeaconInstance

	// feature flags
	ffAllowSyncingBeaconNode bool
}

func NewMultiBeaconClient(log *logrus.Entry, beaconInstances []IBeaconInstance) *MultiBeaconClient {
	client := &MultiBeaconClient{
		log:                      log.WithField("component", "beaconClient"),
		beaconInstances:          beaconInstances,
		bestBeaconIndex:          *uberatomic.NewInt64(0),
		ffAllowSyncingBeaconNode: false,
	}

	// feature flags
	if os.Getenv("ALLOW_SYNCING_BEACON_NODE") != "" {
		client.log.Warn("env: ALLOW_SYNCING_BEACON_NODE: allow syncing beacon node")
		client.ffAllowSyncingBeaconNode = true
	}

	return client
}

func (c *MultiBeaconClient) BestSyncStatus() (*SyncStatusPayloadData, error) {
	var bestSyncStatus *SyncStatusPayloadData
	var foundSyncedNode bool

	// Check each beacon-node sync status
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, instance := range c.beaconInstances {
		wg.Add(1)
		go func(instance IBeaconInstance) {
			defer wg.Done()
			log := c.log.WithField("uri", instance.GetURI())
			log.Debug("getting sync status")

			syncStatus, err := instance.SyncStatus()
			if err != nil {
				log.WithError(err).Error("failed to get sync status")
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if foundSyncedNode {
				return
			}

			if bestSyncStatus == nil {
				bestSyncStatus = syncStatus
			}

			if !syncStatus.IsSyncing {
				bestSyncStatus = syncStatus
				foundSyncedNode = true
			}
		}(instance)
	}

	// Wait for all requests to complete...
	wg.Wait()

	if !foundSyncedNode && !c.ffAllowSyncingBeaconNode {
		return nil, ErrBeaconNodeSyncing
	}

	if bestSyncStatus == nil {
		return nil, ErrBeaconNodesUnavailable
	}

	return bestSyncStatus, nil
}

// SubscribeToHeadEvents subscribes to head events from all beacon nodes. A single head event will be received multiple times,
// likely once for every beacon nodes.
func (c *MultiBeaconClient) SubscribeToHeadEvents(slotC chan HeadEventData) {
	for _, instance := range c.beaconInstances {
		go instance.SubscribeToHeadEvents(slotC)
	}
}

// SubscribeToPayloadAttributes subscribes to payload_attributes events from all beacon nodes. A single head event will be received multiple times,
// likely once for every beacon nodes.
func (c *MultiBeaconClient) SubscribeToPayloadAttributes(payloadAttributesChan chan PayloadAttributesData) {
	for _, instance := range c.beaconInstances {
		go instance.SubscribeToPayloadAttributes(payloadAttributesChan)
	}
}

func (c *MultiBeaconClient) FetchValidators(headSlot uint64) (map[types.PubkeyHex]ValidatorResponseEntry, error) {
	// return the first successful beacon node response
	clients := c.beaconInstancesByLastResponse()

	for i, client := range clients {
		log := c.log.WithField("uri", client.GetURI())
		log.Debug("fetching validators")

		validators, err := client.FetchValidators(headSlot)
		if err != nil {
			log.WithError(err).Error("failed to fetch validators")
			continue
		}

		c.bestBeaconIndex.Store(int64(i))

		// Received successful response. Set this index as last successful beacon node
		return validators, nil
	}

	return nil, ErrBeaconNodesUnavailable
}

func (c *MultiBeaconClient) GetProposerDuties(epoch uint64) (*ProposerDutiesResponse, error) {
	// return the first successful beacon node response
	clients := c.beaconInstancesByLastResponse()
	log := c.log.WithField("epoch", epoch)

	for i, client := range clients {
		log := log.WithField("uri", client.GetURI())
		log.Debug("fetching proposer duties")

		duties, err := client.GetProposerDuties(epoch)
		if err != nil {
			log.WithError(err).Error("failed to get proposer duties")
			continue
		}

		c.bestBeaconIndex.Store(int64(i))

		// Received successful response. Set this index as last successful beacon node
		return duties, nil
	}

	return nil, ErrBeaconNodesUnavailable
}

// beaconInstancesByLastResponse returns a list of beacon clients that has the client
// with the last successful response as the first element of the slice
func (c *MultiBeaconClient) beaconInstancesByLastResponse() []IBeaconInstance {
	index := c.bestBeaconIndex.Load()
	if index == 0 {
		return c.beaconInstances
	}

	instances := make([]IBeaconInstance, len(c.beaconInstances))
	copy(instances, c.beaconInstances)
	instances[0], instances[index] = instances[index], instances[0]

	return instances
}

type publishResp struct {
	index int
	code  int
	err   error
}

// GetBeaconNodeURLs returns a list of beacon client URLs ordered by last successful client response
func (c *MultiBeaconClient) GetBeaconNodeURLs() []string {
	clients := c.beaconInstancesByLastResponse()
	urls := make([]string, len(clients))

	for index, client := range clients {
		urls[index] = client.GetURI()
	}

	return urls
}

// GetWithdrawals - 3500/eth/v1/beacon/states/<slot>/withdrawals
func (c *MultiBeaconClient) GetWithdrawals(slot uint64) (withdrawalsResp *GetWithdrawalsResponse, err error) {
	clients := c.beaconInstancesByLastResponse()
	for _, client := range clients {
		log := c.log.WithField("uri", client.GetURI())
		if withdrawalsResp, err = client.GetWithdrawals(slot); err != nil {
			if strings.Contains(err.Error(), "Withdrawals not enabled before capella") {
				break
			}
			log.WithField("slot", slot).WithError(err).Warn("failed to get withdrawals")
			continue
		}

		return withdrawalsResp, nil
	}

	if strings.Contains(err.Error(), "Withdrawals not enabled before capella") {
		c.log.WithField("slot", slot).WithError(err).Debug("failed to get withdrawals as capella has not been reached")
		return nil, errors.New("cannot fetch withdrawals before capella has been reached")
	}

	c.log.WithField("slot", slot).WithError(err).Warn("failed to get withdrawals from any CL node")
	return nil, err
}

// PublishBlock publishes the signed beacon block via https://ethereum.github.io/beacon-APIs/#/ValidatorRequiredApi/publishBlock
func (c *MultiBeaconClient) PublishBlock(block *capella.SignedBeaconBlock) (code int, err error) {
	log := c.log.WithFields(logrus.Fields{
		"slot":      block.Message.Slot,
		"blockHash": block.Message.Body.ExecutionPayload.BlockHash.String(),
	})

	clients := c.beaconInstancesByLastResponse()
	// The chan will be cleaner up automatically once the function exists even if it was still being written to
	resChans := make(chan publishResp, len(clients))

	for i, client := range clients {
		log := log.WithField("uri", client.GetURI())
		log.Debug("publishing block")
		go func(index int, client IBeaconInstance) {
			code, err := client.PublishBlock(block)
			resChans <- publishResp{
				index: index,
				code:  code,
				err:   err,
			}
		}(i, client)
	}

	var lastErrPublishResp publishResp
	for i := 0; i < len(clients); i++ {
		res := <-resChans
		if res.err != nil {
			log.WithField("beacon", clients[res.index].GetURI()).WithField("statusCode", res.code).WithError(res.err).Error("failed to publish block")
			lastErrPublishResp = res
			continue
		} else if res.code == 202 {
			// Should the block fail full validation, a separate success response code (202) is used to indicate that the block was successfully broadcast but failed integration.
			// https://ethereum.github.io/beacon-APIs/?urls.primaryName=dev#/Beacon/publishBlock
			log.WithField("beacon", clients[res.index].GetURI()).WithField("statusCode", res.code).WithError(res.err).Error("block failed validation but was still broadcast")
			lastErrPublishResp = res
			continue
		}

		c.bestBeaconIndex.Store(int64(res.index))

		log.WithField("beacon", clients[res.index].GetURI()).WithField("statusCode", res.code).Info("published block")
		return res.code, nil
	}

	log.Error("failed to publish block on any CL node")
	return lastErrPublishResp.code, lastErrPublishResp.err
}

// GetSlotBlock - /eth/v2/beacon/blocks/<slot>
func (c *MultiBeaconClient) GetSlotBlock(slot uint64) (res *common.BeaconBlockMessage, err error) {
	clients := c.beaconInstancesByLastResponse()
	for _, client := range clients {
		log := c.log.WithField("uri", client.GetURI())
		if res, err = client.GetSlotBlock(slot); err != nil {
			log.WithField("slot", slot).WithError(err).Warn("failed to get randao")
			continue
		}

		return res, nil
	}

	c.log.WithField("slot", slot).WithError(err).Warn("failed to get randao from any CL node")
	return nil, err
}
