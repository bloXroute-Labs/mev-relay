package server

import (
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
)

const (
	configFilesUpdateInterval = 30 * time.Minute

	/*
		The high_priority_builder_data.json file will have two sections, each a map[string]bool, one for account builder
		public keys, and one for builder bloXroute account IDs. The builder public keys in this file can be merged with
		public keys supplied in relay startup arguments, so if we need to "delete" a key, the bool value in the file
		should be changed to "false" instead of deleting the entry.
		Example file format:
		{
			"high_priority_builder_pubkeys": {"0xabcdef" : true},
			"high_priority_builder_account_ids": {"11111111-1111-1111-1111-111111111111" : true}
		}
	*/
	highPriorityBuilderDataConfigFile = "config_files/high_priority_builder_data.json"
	trustedValidatorDataConfigFile    = "config_files/trusted_validator_data.json"
	weiToEthMultiplier                = 1e18
)

type TrustedValidatorData struct {
	TrustedValidatorBearerTokens map[string]struct{} `json:"trusted_validator_bearer_tokens"`
}

// loadTrustedValidatorData loads trusted validator data from config file
func (m *BoostService) loadTrustedValidatorData() {
	f, err := os.Open(trustedValidatorDataConfigFile)
	if err != nil {
		m.log.Errorf("could not load trusted validator data, %v", err)
		return
	}

	var trustedValidatorData TrustedValidatorData
	if err := json.NewDecoder(f).Decode(&trustedValidatorData); err != nil {
		m.log.Errorf("could not load trusted validator data, %v", err)
		return
	}

	// move trusted validator bearer tokens from config to existing map
	for k, v := range trustedValidatorData.TrustedValidatorBearerTokens {
		m.trustedValidatorBearerTokens.Store(strings.ToLower(k), v)
	}

	m.log.WithField("TrustedValidatorBearerTokenSize", m.trustedValidatorBearerTokens.Size()).Info("trusted validator data successfully loaded")
}

type HighPriorityBuilderData struct {
	Pubkeys                             map[string]bool    `json:"high_priority_builder_pubkeys"`
	AccountIds                          map[string]bool    `json:"high_priority_builder_account_ids"`
	PubkeysToSkipSimulationThreshold    map[string]float64 `json:"high_priority_builder_pubkeys_to_skip_simulation_threshold"`
	AccountIdsToSkipSimulationThreshold map[string]float64 `json:"high_priority_builder_account_ids_to_skip_simulation_threshold"`
	UltraTierAccIDs                     map[string]bool    `json:"ultra_tier_builder_account_ids"`
	NoRateLimitAccIDs                   map[string]bool    `json:"no_rate_limit_builder_account_ids"`
}

// loadHighPriorityBuilderData loads high priority builder public keys from config file
func (m *BoostService) loadHighPriorityBuilderData() {
	f, err := os.Open(highPriorityBuilderDataConfigFile)
	if err != nil {
		m.log.Errorf("could not load high priority builder data, %v", err)
		return
	}

	var highPriorityBuilderData HighPriorityBuilderData
	if err := json.NewDecoder(f).Decode(&highPriorityBuilderData); err != nil {
		m.log.Errorf("could not load high priority builder data, %v", err)
		return
	}

	m.highPriorityBuilderDataLock.Lock()
	// merging builder keys with existing map
	for key, value := range highPriorityBuilderData.Pubkeys {
		m.highPriorityBuilderPubkeys.Store(key, value)
	}
	m.ultraBuilderAccountIDs = convertToStringSyncmap(highPriorityBuilderData.UltraTierAccIDs)
	m.noRateLimitUltraBuilderAccIDs = convertToStringSyncmap(highPriorityBuilderData.NoRateLimitAccIDs)
	// don't need to iterate over account ID's since we're not merging with existing data
	m.highPriorityBuilderAccountIDs = convertToStringSyncmap(highPriorityBuilderData.AccountIds)

	// convert eth values found in config file to wei
	pubkeysToSkipSimulationThreshold := syncmap.NewStringMapOf[*big.Int]()
	for key, value := range highPriorityBuilderData.PubkeysToSkipSimulationThreshold {
		pubkeysToSkipSimulationThreshold.Store(key, big.NewInt(int64(value*weiToEthMultiplier)))
	}
	m.highPriorityBuilderPubkeysToSkipSimulationThreshold = pubkeysToSkipSimulationThreshold

	// convert eth values found in config file to wei
	accountIDsToSkipSimulationThreshold := syncmap.NewStringMapOf[*big.Int]()
	for key, value := range highPriorityBuilderData.AccountIdsToSkipSimulationThreshold {
		accountIDsToSkipSimulationThreshold.Store(key, big.NewInt(int64(value*weiToEthMultiplier)))
	}
	m.highPriorityBuilderAccountIDsToSkipSimulationThreshold = accountIDsToSkipSimulationThreshold

	m.highPriorityBuilderDataLock.Unlock()

	m.log.Info("high priority builder data successfully loaded")
}
