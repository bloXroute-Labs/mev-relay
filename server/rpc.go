package server

import (
	"context"
	"errors"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/services/statistics"
	relaygrpc "github.com/bloXroute-Labs/relay-grpc"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/peer"
)

func (m *BoostService) GRPCAuthentication(ctx context.Context) (context.Context, error) {
	return m.auth.rpcAuthentication(ctx)
}

func (m *BoostService) SubmitBlock(ctx context.Context, block *relaygrpc.SubmitBlockRequest) (*relaygrpc.SubmitBlockResponse, error) {
	start := time.Now()

	p, _ := peer.FromContext(ctx)
	clientIPAddress := p.Addr.String()

	span := trace.SpanFromContext(ctx)
	defer span.End()

	log := m.log.WithFields(logrus.Fields{
		"builderIP":       clientIPAddress,
		"clientIPAddress": clientIPAddress,
		"method":          "submitNewBlock-RPC",
		"traceId":         span.SpanContext().TraceID(),
	})

	capellaBlock := relaygrpc.ProtoRequestToCapellaRequest(block)

	if capellaBlock == nil || capellaBlock.Message == nil {
		return nil, errors.New("invalid submission")
	}

	span.SetAttributes(
		attribute.String("method", "submitBlock-RPC"),
		attribute.String("blockHash", capellaBlock.Message.BlockHash.String()),
	)

	var tier sdnmessage.AccountTier
	var externalBuilderAccountID string
	var isExternalBuilder bool

	if checkThatRequestAuthorizedBy(ctx, headerAuth) {
		isExternalBuilder = true
		externalBuilderAccountID = getInfoFromRequest(ctx, accountIDKey).(string)
		authInfo, ok := ctx.Value(authInfoKey).(authInfo)
		if !ok {
			return nil, errors.New("could not get auth info")
		}

		tierValue, ok := authInfo[accountTier].(sdnmessage.AccountTier)
		if !ok {
			return nil, errors.New("invalid tier")
		}
		tier = tierValue
	} else {
		tier = sdnmessage.ATierUltra
	}

	var stat builderBlockReceivedStatsRecord
	_, logSpan := m.tracer.Start(ctx, "fluentdLogger")
	if m.stats.NodeID != "" {
		stat = m.createBuilderBlockReceivedStatsRecord(clientIPAddress, externalBuilderAccountID, capellaBlock, isExternalBuilder)

		defer func() {
			m.stats.LogToFluentD(
				statistics.Record{
					Data: &stat,
					Type: "NewBlockReceivedFromBuilder",
				},
				time.Now(),
				statsNameNewBlockFromBuilder)
		}()
	}
	logSpan.End()

	if err := m.handleBlockPayload(ctx, log, capellaBlock, &stat, externalBuilderAccountID, isExternalBuilder, start, clientIPAddress, clientIPAddress, start, tier); err != nil {
		return nil, err
	}

	return &relaygrpc.SubmitBlockResponse{
		Status: "successful",
	}, nil
}
