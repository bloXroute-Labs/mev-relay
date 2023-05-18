package common

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/stretchr/testify/require"
)

func TestMakePostRequest(t *testing.T) {
	// Test errors
	var x chan bool
	resp, err := MakeRequest(context.Background(), *http.DefaultClient, http.MethodGet, "", x)
	require.Error(t, err)
	require.Nil(t, resp)

	// To satisfy the bodyclose linter.
	if resp != nil {
		resp.Body.Close()
	}
}

func TestDecodeExtraData(t *testing.T) {
	bloxrouteExtraHex := "0x506f776572656420627920626c6f58726f757465"
	bloxrouteExtraData := types.ExtraData(common.Hex2Bytes(strings.TrimPrefix(bloxrouteExtraHex, "0x")))
	expected := "Powered by bloXroute"

	require.Equal(t, bloxrouteExtraData.String(), bloxrouteExtraHex)

	result := DecodeExtraData(bloxrouteExtraData)
	require.Equal(t, expected, result)

	badHexExtraData := types.ExtraData(common.Hex2Bytes("0x506f77ffffff0627920ffff26f757465"))
	result = DecodeExtraData(badHexExtraData)
	require.Equal(t, "", result)

	emptyExtraData := types.ExtraData(common.Hex2Bytes(""))
	result = DecodeExtraData(emptyExtraData)
	require.Equal(t, "", result)
}
