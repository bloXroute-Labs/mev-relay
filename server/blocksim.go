package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/flashbots/go-utils/jsonrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var (
	ErrRequestClosed    = errors.New("request context closed")
	ErrSimulationFailed = errors.New("simulation failed")
)

func sendSim(ctx context.Context, payload *common.BuilderBlockValidationRequest, executionNodeURL string) error {
	method := "flashbots_validateBuilderSubmissionV2"
	simReq := jsonrpc.NewJSONRPCRequest("1", method, payload)
	simResp, err := SendJSONRPCRequest(ctx, *simReq, executionNodeURL)
	if err != nil {
		return err
	} else if simResp.Error != nil {
		return fmt.Errorf("%w: %s", ErrSimulationFailed, simResp.Error.Message)
	}

	return nil
}

// SendJSONRPCRequest sends the request to URL and returns the general JsonRpcResponse, or an error (note: not the JSONRPCError)
func SendJSONRPCRequest(ctx context.Context, req jsonrpc.JSONRPCRequest, url string) (res *jsonrpc.JSONRPCResponse, err error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := otelhttp.Post(ctx, url, "application/json", bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res = new(jsonrpc.JSONRPCResponse)
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		return nil, err
	}

	return res, nil
}
