package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/flashbots/go-boost-utils/types"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrActiveValidatorNotFound       = errors.New("active validator not found")
	ErrValidatorRegistrationNotFound = errors.New("validator registration not found")
)

func (ds *Datastore) SetActiveValidators(ctx context.Context, validators map[string]interface{}) error {

	pipe := ds.redis.client.Pipeline()

	for key := range validators {
		pipe.Set(ctx, ds.redis.keyActiveValidator(key), 1, time.Hour)
	}

	_, err := pipe.Exec(ctx)

	return err
}

func (ds *Datastore) GetActiveValidators(ctx context.Context) (int, error) {
	val, err := ds.redis.client.Get(ctx, ds.redis.prefixActiveValidatorsInt).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	if val == "" {
		return 0, nil
	}
	return strconv.Atoi(val)
}

// GetActiveValidator returns the active validator with the given public key.
func (ds *Datastore) GetActiveValidator(ctx context.Context, pubKey string) (*ValidatorLatency, error) {
	key := ds.redis.keyValidatorRegistration
	entry, err := ds.redis.client.HGet(ctx, key, pubKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if entry == "" {
		return nil, ErrActiveValidatorNotFound
	}

	var v ValidatorLatency
	err = json.Unmarshal([]byte(entry), &v)

	return &v, err
}

func (ds *Datastore) SetValidatorRegistrationMap(ctx context.Context, data map[string]interface{}) error {
	return ds.redis.client.HSet(ctx, ds.redis.keyValidatorRegistration, data).Err()
}

// GetValidatorRegistrations returns the validator registrations for the given proposerPubKeys.
func (ds *Datastore) GetValidatorRegistrations(ctx context.Context, proposerPubKeys []string) ([]types.SignedValidatorRegistration, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("GetValidatorRegistrations")
	defer span.End()
	// if no proposer pub keys are provided, return an empty slice.
	if len(proposerPubKeys) == 0 {
		span.SetStatus(codes.Error, "no pubkeys provided")
		return nil, nil
	}

	values, err := ds.redis.client.HMGet(ctx, ds.redis.keyValidatorRegistration, proposerPubKeys...).Result()
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var registrations []types.SignedValidatorRegistration
	for i := range values {
		strVal, ok := values[i].(string)
		if !ok || strVal == "" {
			// skip empty values
			continue
		}

		var registration ValidatorLatency
		err = json.Unmarshal([]byte(strVal), &registration)
		if err != nil {
			// don't return an error if we can't unmarshal validator registration
			ds.log.WithError(err).WithFields(logrus.Fields{
				"proposerPubKey": proposerPubKeys[i],
				"strVal":         strVal,
			}).Error("failed to unmarshal validator registration")
			continue
		}
		if registration.Registration.Message == nil {
			// skip nil registration message
			ds.log.WithError(err).WithFields(logrus.Fields{
				"proposerPubKey": proposerPubKeys[i],
				"strVal":         strVal,
				"lastRegistered": registration.LastRegistered,
				"latency":        registration.Latency,
				"signature":      registration.Registration.Signature,
			}).Error("skipping nil register validator request message")
			continue
		}
		registrations = append(registrations, registration.Registration)
	}
	return registrations, nil
}

// GetValidatorRegistration returns the validator registration for the given proposerPubkey.
func (ds *Datastore) GetValidatorRegistration(ctx context.Context, proposerPubkey types.PubkeyHex) (*types.SignedValidatorRegistration, error) {
	value, err := ds.redis.client.HGet(ctx, ds.redis.keyValidatorRegistration, strings.ToLower(proposerPubkey.String())).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrValidatorRegistrationNotFound
	} else if err != nil {
		return nil, err
	}

	registration := new(ValidatorLatency)
	err = json.Unmarshal([]byte(value), registration)
	fmt.Printf("stuff: %+v", registration)
	return &registration.Registration, err
}
