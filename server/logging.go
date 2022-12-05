package server

import (
	"context"
	"encoding/json"

	"net/http"
	"strings"
	"time"

	"github.com/cornelk/hashmap"
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
func (m *RelayService) StartActivityLogger(parent context.Context) {
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

func (m *RelayService) logPerformance() {
	endInterval := time.Now()
	performanceStatsRecord := m.performanceStats.CloseInterval(endInterval)
	if performanceStatsRecord.StartTime != "0001-01-01T00:00:00.000000" {
		record := map[string]interface{}{
			"data": performanceStatsRecord,
			"type": "BuilderRelayPerformance",
		}
		m.log.Info(record, endInterval, "builder-relay.stats.performance")
	}
}

func (m *RelayService) logValidatorActivity() {
	m.log.WithField("validator-activity", m.validatorActivity.String()).Info("calls made within the last 30 minutes")
	m.validatorActivity = hashmap.New[string, validatorActivity]()
}

func (m *RelayService) logRelayActivity() {
	if data, err := json.Marshal(m.relayRankings); err != nil {
		m.log.WithError(err).Warn("could not json marshal relay rankings")
		m.log.WithField("relay-rankings", m.relayRankings).Info("relay rankings within the last 30 minutes")
	} else {
		m.log.WithField("relay-rankings", string(data)).Info("relay rankings within the last 30 minutes")
	}
	m.relayRankings = make(map[string]relayRanking)
}

func (m *RelayService) trackActivity(path string, r *http.Request) {

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
		activity, ok := m.validatorActivity.Get(id)
		if !ok {
			m.validatorActivity.Set(id, validatorActivity{
				LastCall: time.Now(),
				Activity: hashmap.New[string, requests](),
			})
		} else {
			m.validatorActivity.Set(id, validatorActivity{
				LastCall: time.Now(),
				Activity: activity.Activity,
			})
		}
		return
	}

	for _, pubkey := range pubkeys {
		activity, ok := m.validatorActivity.Get(id)
		if !ok {
			req := hashmap.New[string, requests]()
			count := hashmap.New[string, int]()
			count.Set(path, 1)
			req.Set(pubkey, requests{
				Count: count,
			})
			m.validatorActivity.Set(id, validatorActivity{
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
