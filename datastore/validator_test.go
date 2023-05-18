package datastore

import (
	"context"
	"encoding/binary"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatastore_SetValidatorRegistrationMap(t *testing.T) {
	ds := setupTestDatastore(t)
	domain := types.ComputeDomain(types.DomainType{0x01, 0x00, 0x00, 0x00}, types.ForkVersion{}, types.Root{})
	val1 := genValidatorRegistration(t, domain)
	val2 := genValidatorRegistration(t, domain)
	val3 := genValidatorRegistration(t, domain)

	validators := map[string]interface{}{
		strings.ToLower(val1.Message.Pubkey.PubkeyHex().String()): &ValidatorLatency{Registration: val1},
		strings.ToLower(val2.Message.Pubkey.PubkeyHex().String()): &ValidatorLatency{Registration: val2},
	}

	err := ds.SetValidatorRegistrationMap(context.Background(), validators)
	require.NoError(t, err)

	res, err := ds.GetValidatorRegistration(context.Background(), val1.Message.Pubkey.PubkeyHex())
	require.NoError(t, err)
	assert.Equal(t, val1, *res)

	_, err = ds.GetValidatorRegistration(context.Background(), val3.Message.Pubkey.PubkeyHex())
	require.ErrorIs(t, err, ErrValidatorRegistrationNotFound)
}

func TestDatastore_GetValidatorRegistrations(t *testing.T) {
	ds := setupTestDatastore(t)
	domain := types.ComputeDomain(types.DomainType{0x01, 0x00, 0x00, 0x00}, types.ForkVersion{}, types.Root{})
	val1 := genValidatorRegistration(t, domain)
	val2 := genValidatorRegistration(t, domain)
	val3 := genValidatorRegistration(t, domain)
	val4 := genValidatorRegistration(t, domain)

	validators := map[string]interface{}{
		strings.ToLower(val1.Message.Pubkey.PubkeyHex().String()): &ValidatorLatency{Registration: val1},
		strings.ToLower(val2.Message.Pubkey.PubkeyHex().String()): &ValidatorLatency{Registration: val2},
		strings.ToLower(val3.Message.Pubkey.PubkeyHex().String()): &ValidatorLatency{Registration: val3},
	}

	err := ds.SetValidatorRegistrationMap(context.Background(), validators)
	require.NoError(t, err)

	t.Run("get all", func(t *testing.T) {
		res, err := ds.GetValidatorRegistrations(context.Background(),
			[]string{
				val1.Message.Pubkey.PubkeyHex().String(),
				val2.Message.Pubkey.PubkeyHex().String(),
				val3.Message.Pubkey.PubkeyHex().String(),
				val4.Message.Pubkey.PubkeyHex().String(),
			},
		)
		require.NoError(t, err)
		require.Len(t, res, len(validators))
	})
	t.Run("one non-existing key", func(t *testing.T) {
		res, err := ds.GetValidatorRegistrations(context.Background(),
			[]string{val4.Message.Pubkey.PubkeyHex().String()},
		)
		require.NoError(t, err)
		require.Len(t, res, 0)
	})
}

func TestDatastore_SetActiveValidators(t *testing.T) {
	ds := setupTestDatastore(t)
	domain := types.ComputeDomain(types.DomainType{0x01, 0x00, 0x00, 0x00}, types.ForkVersion{}, types.Root{})
	val1 := genValidatorRegistration(t, domain)
	val2 := genValidatorRegistration(t, domain)
	validators := map[string]interface{}{
		"test1": &ValidatorLatency{Registration: val1, LastRegistered: time.Now().UnixNano()},
		"test2": &ValidatorLatency{Registration: val2, LastRegistered: time.Now().UnixNano()},
	}

	err := ds.SetValidatorRegistrationMap(context.Background(), validators)
	require.NoError(t, err)

	err = ds.UpdateActiveValidators()
	require.NoError(t, err)

	count, err := ds.GetActiveValidators(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func generateIPAddress() string {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, rand.Uint32())
	return net.IP(buf).String()
}

func generateKeyPair(t require.TestingT) (types.PublicKey, *bls.SecretKey) {
	sk, pk, err := bls.GenerateNewKeypair()
	require.NoError(t, err)

	var pubKey types.PublicKey
	err = pubKey.FromSlice(pk.Compress())
	require.NoError(t, err)

	return pubKey, sk
}

func genValidatorRegistration(t require.TestingT, domain types.Domain) types.SignedValidatorRegistration {
	pubKey, sk := generateKeyPair(t)

	msg := &types.RegisterValidatorRequestMessage{
		FeeRecipient: types.Address{0x42},
		GasLimit:     15_000_000,
		Timestamp:    1652369368,
		Pubkey:       pubKey,
	}

	signature, err := types.SignMessage(msg, domain, sk)
	require.NoError(t, err)

	return types.SignedValidatorRegistration{
		Message:   msg,
		Signature: signature,
	}
}
