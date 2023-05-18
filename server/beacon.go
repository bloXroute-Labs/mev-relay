package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	capellaapi "github.com/attestantio/go-eth2-client/api/v1/capella"
	"github.com/attestantio/go-eth2-client/spec/capella"
)

var ErrHTTPErrorResponse = errors.New("got an HTTP error response")

func fetchBeacon(method, url string, payload, dst any) (code int, err error) {
	var req *http.Request

	if payload == nil {
		req, err = http.NewRequest(method, url, nil)
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		if err2 != nil {
			return 0, fmt.Errorf("could not marshal request: %w", err2)
		}
		req, err = http.NewRequest(method, url, bytes.NewReader(payloadBytes))

		// Set content-type
		req.Header.Add("Content-Type", "application/json")
	}

	if err != nil {
		return 0, fmt.Errorf("invalid request for %s: %w", url, err)
	}
	req.Header.Set("accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("client refused for %s: %w", url, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("could not read response body for %s: %w", url, err)
	}

	if resp.StatusCode >= http.StatusMultipleChoices {
		ec := &struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}{}
		if err = json.Unmarshal(bodyBytes, ec); err != nil {
			return resp.StatusCode, fmt.Errorf("could not unmarshal error response from beacon node for %s from %s: %w", url, string(bodyBytes), err)
		}
		return resp.StatusCode, fmt.Errorf("%w: %s", ErrHTTPErrorResponse, ec.Message)
	}

	if dst != nil {
		err = json.Unmarshal(bodyBytes, dst)
		if err != nil {
			return resp.StatusCode, fmt.Errorf("could not unmarshal response for %s from %s: %w", url, string(bodyBytes), err)
		}
	}

	return resp.StatusCode, nil
}

func (m *BoostService) publishBlock(signedBlindedBeaconBlock *capellaapi.SignedBlindedBeaconBlock, executionPayload *capella.ExecutionPayload) error {
	signedBeaconBlock := SignedBlindedBeaconBlockToBeaconBlock(signedBlindedBeaconBlock, executionPayload)
	code, err := m.beaconClient.PublishBlock(signedBeaconBlock)
	if err != nil {
		m.log.WithError(err).Warn("could not publish block")
		return err
	}

	m.log.WithField("code", code).Info("published block")
	return nil
}

type GetGenesisResponse struct {
	Data struct {
		GenesisTime           uint64 `json:"genesis_time,string"`
		GenesisValidatorsRoot string `json:"genesis_validators_root"`
		GenesisForkVersion    string `json:"genesis_fork_version"`
	}
}

// GetGenesis returns the genesis info - https://ethereum.github.io/beacon-APIs/#/Beacon/getGenesis
func GetGenesis(url string) (*GetGenesisResponse, error) {
	uri := fmt.Sprintf("%s/eth/v1/beacon/genesis", url)
	resp := new(GetGenesisResponse)
	_, err := fetchBeacon(http.MethodGet, uri, nil, resp)
	return resp, err
}

// GetGenesis returns the genesis info - https://ethereum.github.io/beacon-APIs/#/Beacon/getGenesis
func (m *BoostService) GetGenesis() (genesisInfo *GetGenesisResponse, err error) {
	clients := m.beaconClient.GetBeaconNodeURLs()
	for _, client := range clients {
		log := m.log.WithField("uri", client)
		log.Debug("publishing block")

		if genesisInfo, err = GetGenesis(client); err != nil {
			log.WithError(err).Warn("failed to get genesis info")
			continue
		}

		return genesisInfo, nil
	}

	m.log.WithError(err).Error("failed to publish block on any CL node")
	return genesisInfo, err
}
