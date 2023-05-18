package common

import (
	"encoding/json"
	"errors"

	"github.com/attestantio/go-builder-client/api"
	"github.com/attestantio/go-builder-client/api/capella"
	"github.com/attestantio/go-builder-client/spec"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	consensuscapella "github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	utilbellatrix "github.com/attestantio/go-eth2-client/util/bellatrix"
	utilcapella "github.com/attestantio/go-eth2-client/util/capella"
	"github.com/flashbots/go-boost-utils/bls"
	boostTypes "github.com/flashbots/go-boost-utils/types"
)

var (
	ErrMissingRequest     = errors.New("req is nil")
	ErrMissingSecretKey   = errors.New("secret key is nil")
	ErrInvalidTransaction = errors.New("invalid transaction")
)

type HTTPErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

var NilResponse = struct{}{}

var VersionBellatrix boostTypes.VersionString = "bellatrix"

var ZeroU256 = boostTypes.IntToU256(0)

func BuildGetHeaderResponse(payload *BuilderSubmitBlockRequest, sk *bls.SecretKey, pubkey *boostTypes.PublicKey, domain boostTypes.Domain) (*GetHeaderResponse, error) {
	if payload == nil {
		return nil, ErrMissingRequest
	}

	if sk == nil {
		return nil, ErrMissingSecretKey
	}

	if payload.Capella != nil {
		signedBuilderBid, err := BuilderSubmitBlockRequestToSignedBuilderBid(payload.Capella, sk, (*phase0.BLSPubKey)(pubkey), domain)
		if err != nil {
			return nil, err
		}
		return &GetHeaderResponse{
			Capella: &spec.VersionedSignedBuilderBid{
				Version:   consensusspec.DataVersionCapella,
				Capella:   signedBuilderBid,
				Bellatrix: nil,
			},
			Bellatrix: nil,
		}, nil
	}
	return nil, ErrEmptyPayload
}

func BuildGetPayloadResponse(payload *BuilderSubmitBlockRequest) (*GetPayloadResponse, error) {
	if payload.Capella != nil {
		return &GetPayloadResponse{
			Capella: &api.VersionedExecutionPayload{
				Version:   consensusspec.DataVersionCapella,
				Capella:   payload.Capella.ExecutionPayload,
				Bellatrix: nil,
			},
			Bellatrix: nil,
		}, nil
	}

	return nil, ErrEmptyPayload
}

func BuilderSubmitBlockRequestToSignedBuilderBid(req *capella.SubmitBlockRequest, sk *bls.SecretKey, pubkey *phase0.BLSPubKey, domain boostTypes.Domain) (*capella.SignedBuilderBid, error) {
	header, err := PayloadToPayloadHeader(req.ExecutionPayload)
	if err != nil {
		return nil, err
	}

	builderBid := capella.BuilderBid{
		Value:  req.Message.Value,
		Header: header,
		Pubkey: *pubkey,
	}

	sig, err := boostTypes.SignMessage(&builderBid, domain, sk)
	if err != nil {
		return nil, err
	}

	return &capella.SignedBuilderBid{
		Message:   &builderBid,
		Signature: phase0.BLSSignature(sig),
	}, nil
}

func PayloadToPayloadHeader(p *consensuscapella.ExecutionPayload) (*consensuscapella.ExecutionPayloadHeader, error) {
	if p == nil {
		return nil, ErrEmptyPayload
	}

	transactions := utilbellatrix.ExecutionPayloadTransactions{Transactions: p.Transactions}
	transactionsRoot, err := transactions.HashTreeRoot()
	if err != nil {
		return nil, err
	}

	withdrawals := utilcapella.ExecutionPayloadWithdrawals{Withdrawals: p.Withdrawals}
	withdrawalsRoot, err := withdrawals.HashTreeRoot()
	if err != nil {
		return nil, err
	}

	return &consensuscapella.ExecutionPayloadHeader{
		ParentHash:       p.ParentHash,
		FeeRecipient:     p.FeeRecipient,
		StateRoot:        p.StateRoot,
		ReceiptsRoot:     p.ReceiptsRoot,
		LogsBloom:        p.LogsBloom,
		PrevRandao:       p.PrevRandao,
		BlockNumber:      p.BlockNumber,
		GasLimit:         p.GasLimit,
		GasUsed:          p.GasUsed,
		Timestamp:        p.Timestamp,
		ExtraData:        p.ExtraData,
		BaseFeePerGas:    p.BaseFeePerGas,
		BlockHash:        p.BlockHash,
		TransactionsRoot: transactionsRoot,
		WithdrawalsRoot:  withdrawalsRoot,
	}, nil
}

type BuilderBlockValidationRequest struct {
	BuilderSubmitBlockRequest
	RegisteredGasLimit uint64 `json:"registered_gas_limit,string"`
}

func (r *BuilderBlockValidationRequest) MarshalJSON() ([]byte, error) {
	blockRequest, err := r.BuilderSubmitBlockRequest.MarshalJSON()
	if err != nil {
		return nil, err
	}
	gasLimit, err := json.Marshal(&struct {
		RegisteredGasLimit uint64 `json:"registered_gas_limit,string"`
	}{
		RegisteredGasLimit: r.RegisteredGasLimit,
	})
	if err != nil {
		return nil, err
	}
	gasLimit[0] = ','
	return append(blockRequest[:len(blockRequest)-1], gasLimit...), nil
}
