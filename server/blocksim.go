package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/flashbots/go-boost-utils/types"
	"github.com/flashbots/go-utils/jsonrpc"
)

var (
	ErrRequestClosed    = errors.New("request context closed")
	ErrSimulationFailed = errors.New("simulation failed")
)

func sendSim(payload *types.BuilderSubmitBlockRequest, executionNodeURL string) error {
	simReq := jsonrpc.NewJSONRPCRequest("1", "flashbots_validateBuilderSubmissionV1", payload)
	simResp, err := SendJSONRPCRequest(*simReq, executionNodeURL)
	if err != nil {
		return err
	} else if simResp.Error != nil {
		return fmt.Errorf("%w: %s", ErrSimulationFailed, simResp.Error.Message)
	}

	return nil
}

// SendJSONRPCRequest sends the request to URL and returns the general JsonRpcResponse, or an error (note: not the JSONRPCError)
func SendJSONRPCRequest(req jsonrpc.JSONRPCRequest, url string) (res *jsonrpc.JSONRPCResponse, err error) {
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Add("Content-Type", "application/json")

	// execute request
	resp, err := http.DefaultClient.Do(httpReq)
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
