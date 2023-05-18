package server

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/attestantio/go-builder-client/api/capella"
	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/services/statistics"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var megabyte = 1000000

var upgrader = websocket.Upgrader{
	HandshakeTimeout: 2 * time.Second,
	ReadBufferSize:   3 * megabyte,
}

func (m *BoostService) HandleSocketConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		m.respondError(w, http.StatusInternalServerError, "connection was closed")
		return
	}

	go func() {
		if err := m.websocketReader(conn, w, r); err != nil {
			m.log.WithError(err).Info("error within conenction reader")
			m.respondError(w, http.StatusInternalServerError, "connection was closed")
		}
	}()
}

func (m *BoostService) websocketReader(conn *websocket.Conn, w http.ResponseWriter, req *http.Request) error {
	for {
		t, p, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		ctx, span := m.tracer.Start(req.Context(), "handlePostBlock-WebSocket")
		defer span.End()

		_, initialChecks := m.tracer.Start(ctx, "initialChecks")

		if string(p) == "ping" || t == websocket.PingMessage {
			if err := conn.WriteMessage(websocket.PongMessage, []byte("pong")); err != nil {
				initialChecks.End()
				return err
			}
			continue
		}

		start := time.Now().UTC()
		success := false
		defer func() {
			m.performanceStats.SetEndpointStats(pathSubmitNewBlock, uint64(time.Since(start).Microseconds()), success)
		}()

		var isExternalBuilder bool
		var externalBuilderAccountID string

		if checkThatRequestAuthorizedBy(req.Context(), headerAuth) {
			isExternalBuilder = true
			externalBuilderAccountID = getInfoFromRequest(req.Context(), accountIDKey).(string)
		}

		clientIPAddress := common.GetIPXForwardedFor(req)

		log := m.log.WithFields(logrus.Fields{
			"builderIP":       req.RemoteAddr,
			"clientIPAddress": clientIPAddress,
			"method":          "submitNewBlock-Websocket",
		})

		initialChecks.End()

		_, marshalSpan := m.tracer.Start(ctx, "marshalPayload")

		payload := new(capella.SubmitBlockRequest)

		// ToDo check the req.Header
		// if req.Header.Get("Content-Type") == "application/json" {
		if err := payload.UnmarshalSSZ(p); err != nil {
			if err := payload.UnmarshalJSON(p); err != nil {
				log.WithError(err).Errorf("could not decode payload as json or ssz, %v", string(p))
				conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("could not decode payload; %v", err)))
				marshalSpan.End()
				continue
			}
		}

		marshalSpan.End()

		if payload.Message == nil || payload.ExecutionPayload == nil {
			log.Error("skipping due to missing entries in the request")
			conn.WriteMessage(websocket.TextMessage, []byte("skipping due to missing entries in the request; invalid payload: either 'message' or 'execution_payload' are missing"))
			continue
		}

		_, responseWriterSpan := m.tracer.Start(ctx, "responseWriter")
		conn.WriteMessage(websocket.TextMessage, []byte("bid received"))
		responseWriterSpan.End()

		var stat builderBlockReceivedStatsRecord

		_, logSpan := m.tracer.Start(ctx, "fluentdLogger")
		if m.stats.NodeID != "" {
			stat = m.createBuilderBlockReceivedStatsRecord(common.GetIPXForwardedFor(req), externalBuilderAccountID, payload, isExternalBuilder)

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

		if err := m.handleBlockPayload(ctx, log, payload, &stat, externalBuilderAccountID, isExternalBuilder, start, clientIPAddress, req.RemoteAddr, start, sdnmessage.ATierElite); err != nil {
			log.WithError(err).Error("could not save payload")
			continue
		}

	}
}
