package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/services/statistics"
	bxtypes "github.com/bloXroute-Labs/gateway/v2/types"
	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/cornelk/hashmap"
	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/gorilla/mux"
)

// validatorActivity tracks the activity of
// validators making requests to our relay
type validatorActivity struct {
	LastCall time.Time                          `json:"lastCall"`
	Activity *hashmap.HashMap[string, requests] `json:"activity"`
}

type requests struct {
	Count *hashmap.HashMap[string, int] `json:"count"`
}

const relayPerformanceInterval = 1 * time.Minute
const activityPerformanceInterval = 30 * time.Minute

// relayRanking ranks the various relays provided
// during the startup arguments.  Relays are either
// actual relays or builders.
type relayRanking struct {
	HeaderRequests    int `json:"headerRequests"`
	HighestValueCount int `json:"highestValueCount"`
}

// StartActivityLogger starts a loop that outputs
// validator request logs every 30 minutes and relay general performance every 1 minute
func (m *BoostService) StartActivityLogger(parent context.Context) {
	ticker1Minute := time.NewTicker(relayPerformanceInterval)
	ticker30Minutes := time.NewTicker(activityPerformanceInterval)
	for {
		select {
		case <-parent.Done():
			return
		case <-ticker1Minute.C:
			m.logPerformance()
		case <-ticker30Minutes.C:
			m.logValidatorActivity()
			m.logRelayActivity()
		}
	}
}

func (m *BoostService) logPerformance() {
	if m.stats.NodeID == "" {
		return
	}

	endInterval := time.Now()
	performanceStatsRecord := m.performanceStats.CloseInterval(endInterval)
	if performanceStatsRecord.StartTime != "0001-01-01T00:00:00.000000" {
		record := statistics.Record{
			Data: performanceStatsRecord,
			Type: "BuilderRelayPerformance",
		}
		m.stats.LogToFluentD(record, endInterval, statsNamePerformance)
	}
}

func (m *BoostService) logValidatorActivity() {
	activity := make([]string, 0)
	for _, key := range m.validatorActivity.Keys() {
		activity = append(activity, key)
	}
	m.log.WithField("validator-activity", activity).Info("calls made within the last 30 minutes")
	m.validatorActivity = syncmap.NewStringMapOf[validatorActivity]()
}

func (m *BoostService) logRelayActivity() {
	if data, err := json.Marshal(m.relayRankings); err != nil {
		m.log.WithError(err).Warn("could not json marshal relay rankings")
		m.log.WithField("relay-rankings", m.relayRankings).Info("relay rankings within the last 30 minutes")
	} else {
		m.log.WithField("relay-rankings", string(data)).Info("relay rankings within the last 30 minutes")
	}
	m.relayRankings = syncmap.NewStringMapOf[relayRanking]()
}

func (m *BoostService) trackActivity(path string, r *http.Request) {

	if path == "/" || strings.Contains(path, "/static/") {
		return
	}

	if strings.Contains(path, "eth/v1/builder/header") {
		path = "eth/v1/builder/header"
	}

	id := "unknown"
	cookieID, err := r.Cookie("id")
	if err == nil {
		id = cookieID.Value
	}

	pubkeys, err := getPubKeyFromRequest(path, r)
	if err != nil {
		m.log.WithError(err).Error("could not get the pubkey from request")
		return
	}

	// not every request has pub keys available, set latest call for id
	if len(pubkeys) == 0 {
		activity, ok := m.validatorActivity.Load(id)
		if !ok {
			m.validatorActivity.Store(id, validatorActivity{
				LastCall: time.Now(),
				Activity: hashmap.New[string, requests](),
			})
		} else {
			m.validatorActivity.Store(id, validatorActivity{
				LastCall: time.Now(),
				Activity: activity.Activity,
			})
		}
		return
	}

	for _, pubkey := range pubkeys {
		activity, ok := m.validatorActivity.Load(id)
		if !ok {
			req := hashmap.New[string, requests]()
			count := hashmap.New[string, int]()
			count.Set(path, 1)
			req.Set(pubkey, requests{
				Count: count,
			})
			m.validatorActivity.Store(id, validatorActivity{
				LastCall: time.Now(),
				Activity: req,
			})
			continue
		}
		reqs, ok := activity.Activity.Get(pubkey)
		if !ok {
			count := hashmap.New[string, int]()
			count.Set(path, 1)
			activity.Activity.Set(pubkey, requests{
				Count: count,
			})
			continue
		}

		count, ok := reqs.Count.Get(path)
		if !ok {
			reqs.Count.Set(path, 1)
			continue
		}

		reqs.Count.Set(path, count+1)
	}

}

func getPubKeyFromRequest(path string, r *http.Request) ([]string, error) {
	// only get it from header because its too slow to get from the request body twice
	if strings.Contains(path, "eth/v1/builder/header") {
		vars := mux.Vars(r)
		return []string{vars["pubkey"]}, nil
	}
	return []string{}, nil
}

// StartStats adds the fluentDStats data
// to the service
func (m *BoostService) StartStats(fluentdHost, nodeID string) {
	fluentlogger, err := fluent.New(fluent.Config{
		FluentHost:    fluentdHost,
		FluentPort:    24224,
		MarshalAsJSON: true,
		Async:         true,
	})

	if err != nil {
		m.log.Panic()
	}

	n := bxtypes.NodeID(nodeID)

	t := statistics.FluentdStats{
		NodeID:  n,
		FluentD: fluentlogger,
		Lock:    &sync.RWMutex{},
	}
	m.stats = t
}
