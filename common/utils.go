package common

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/attestantio/go-builder-client/api/capella"
	v1 "github.com/attestantio/go-builder-client/api/v1"
	capellaspec "github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

const (
	weiToEthSignificantDigits = 18
)

var (
	ErrInvalidForkVersion = errors.New("invalid fork version")
	ErrHTTPErrorResponse  = errors.New("got an HTTP error response")
	ErrIncorrectLength    = errors.New("incorrect length")
)

// SlotPos returns the slot's position in the epoch (1-based, i.e. 1..32)
func SlotPos(slot uint64) uint64 {
	return (slot % SlotsPerEpoch) + 1
}

func MakeRequest(ctx context.Context, client http.Client, method, url string, payload any) (*http.Response, error) {
	var req *http.Request
	var err error

	if payload == nil {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		if err2 != nil {
			return nil, err2
		}
		req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(payloadBytes))
	}
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode > 299 {
		defer resp.Body.Close()
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return resp, fmt.Errorf("%w: %d / %s", ErrHTTPErrorResponse, resp.StatusCode, string(bodyBytes))
	}

	return resp, nil
}

func WeiToEth(valueString string) string {
	numDigits := len(valueString)
	missing := int(math.Max(0, float64((weiToEthSignificantDigits+1)-numDigits)))
	prefix := "0000000000000000000"[:missing]
	ethValue := prefix + valueString
	decimalIndex := len(ethValue) - weiToEthSignificantDigits
	return ethValue[:decimalIndex] + "." + ethValue[decimalIndex:]
}

func WeiToEthRounded(value string) string {
	f, err := strconv.ParseFloat(WeiToEth(value), 64)
	if err != nil {
		return value
	}
	return fmt.Sprintf("%.6f", f)
}

// ComputeDomain computes the signing domain
func ComputeDomain(domainType types.DomainType, forkVersionHex, genesisValidatorsRootHex string) (domain types.Domain, err error) {
	genesisValidatorsRoot := types.Root(ethcommon.HexToHash(genesisValidatorsRootHex))
	forkVersionBytes, err := hexutil.Decode(forkVersionHex)
	if err != nil || len(forkVersionBytes) != 4 {
		return domain, ErrInvalidForkVersion
	}
	var forkVersion [4]byte
	copy(forkVersion[:], forkVersionBytes[:4])
	return types.ComputeDomain(domainType, forkVersion, genesisValidatorsRoot), nil
}

func GetEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func GetSliceEnv(key string, defaultValue []string) []string {
	if value, ok := os.LookupEnv(key); ok {
		return strings.Split(value, ",")
	}
	return defaultValue
}

func GetIPXForwardedFor(r *http.Request) string {
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		if strings.Contains(forwarded, ",") { // return first entry of list of IPs
			return strings.Split(forwarded, ",")[0]
		}
		return forwarded
	}
	return r.RemoteAddr
}

// GetMevBoostVersionFromUserAgent returns the mev-boost version from an user agent string
// Example ua: "mev-boost/1.0.1 go-http-client" -> returns "1.0.1". If no version is found, returns "-"
func GetMevBoostVersionFromUserAgent(ua string) string {
	parts := strings.Split(ua, " ")
	if strings.HasPrefix(parts[0], "mev-boost") {
		parts2 := strings.Split(parts[0], "/")
		if len(parts2) == 2 {
			return parts2[1]
		}
	}
	return "-"
}

func U256StrToUint256(s types.U256Str) *uint256.Int {
	i := new(uint256.Int)
	i.SetBytes(reverse(s[:]))
	return i
}

func reverse(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := len(dst)/2 - 1; i >= 0; i-- {
		opp := len(dst) - 1 - i
		dst[i], dst[opp] = dst[opp], dst[i]
	}
	return dst
}

// GetEnvStrSlice returns a slice of strings from a comma-separated env var
func GetEnvStrSlice(key string, defaultValue []string) []string {
	if value, ok := os.LookupEnv(key); ok {
		return strings.Split(value, ",")
	}
	return defaultValue
}

func StrToPhase0Pubkey(s string) (ret phase0.BLSPubKey, err error) {
	pubkeyBytes, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return ret, err
	}
	if len(pubkeyBytes) != phase0.PublicKeyLength {
		return ret, ErrIncorrectLength
	}
	copy(ret[:], pubkeyBytes)
	return ret, nil
}

func StrToPhase0Hash(s string) (ret phase0.Hash32, err error) {
	hashBytes, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return ret, err
	}
	if len(hashBytes) != phase0.Hash32Length {
		return ret, ErrIncorrectLength
	}
	copy(ret[:], hashBytes)
	return ret, nil
}

type CreateTestBlockSubmissionOpts struct {
	relaySk bls.SecretKey
	relayPk types.PublicKey
	domain  types.Domain

	Slot           uint64
	ParentHash     string
	ProposerPubkey string
}

func CreateTestBlockSubmission(t *testing.T, builderPubkey string, value *big.Int, opts *CreateTestBlockSubmissionOpts) (payload *BuilderSubmitBlockRequest, getPayloadResponse *GetPayloadResponse, getHeaderResponse *GetHeaderResponse) {
	t.Helper()
	var err error

	slot := uint64(0)
	relaySk := bls.SecretKey{}
	relayPk := types.PublicKey{}
	domain := types.Domain{}
	proposerPk := phase0.BLSPubKey{}
	parentHash := phase0.Hash32{}

	if opts != nil {
		relaySk = opts.relaySk
		relayPk = opts.relayPk
		domain = opts.domain
		slot = opts.Slot

		if opts.ProposerPubkey != "" {
			proposerPk, err = StrToPhase0Pubkey(opts.ProposerPubkey)
			require.NoError(t, err)
		}

		if opts.ParentHash != "" {
			parentHash, err = StrToPhase0Hash(opts.ParentHash)
			require.NoError(t, err)
		}
	}

	builderPk, err := StrToPhase0Pubkey(builderPubkey)
	require.NoError(t, err)

	payload = &BuilderSubmitBlockRequest{ //nolint:exhaustruct
		Capella: &capella.SubmitBlockRequest{
			Message: &v1.BidTrace{ //nolint:exhaustruct
				BuilderPubkey:  builderPk,
				Value:          uint256.MustFromBig(value),
				Slot:           slot,
				ParentHash:     parentHash,
				ProposerPubkey: proposerPk,
			},
			ExecutionPayload: &capellaspec.ExecutionPayload{}, //nolint:exhaustruct
			Signature:        phase0.BLSSignature{},
		},
	}

	getHeaderResponse, err = BuildGetHeaderResponse(payload, &relaySk, &relayPk, domain)
	require.NoError(t, err)

	getPayloadResponse, err = BuildGetPayloadResponse(payload)
	require.NoError(t, err)

	return payload, getPayloadResponse, getHeaderResponse
}
