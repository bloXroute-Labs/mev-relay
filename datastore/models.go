package datastore

import (
	"encoding/json"

	"github.com/flashbots/go-boost-utils/types"
)

type AliasSignedValidatorRegistration types.SignedValidatorRegistration

func (i AliasSignedValidatorRegistration) MarshalBinary() ([]byte, error) {
	return json.Marshal(i)
}

type ValidatorLatency struct {
	Registration   types.SignedValidatorRegistration `json:"registration"`
	IPAddress      string                            `json:"ip_address"`
	LastRegistered int64                             `json:"last_registered"`
	Latency        float32                           `json:"latency"`
	IsTrusted      bool                              `json:"is_trusted"`
}

func (l *ValidatorLatency) MarshalBinary() ([]byte, error) {

	return json.Marshal(l)
}

func (l *ValidatorLatency) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, &l); err != nil {
		return err
	}

	return nil
}
