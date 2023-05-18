package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/gateway/v2/types"
	"github.com/google/uuid"
	"github.com/justinas/alice"
	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/require"
)

func TestAuth_authMiddleware(t *testing.T) {
	accID := uuid.NewString()
	waitAccID := uuid.NewString()
	unauthorizedAccID := uuid.NewString()
	nonExistentAccID := uuid.NewString()
	secret := uuid.NewString()

	sleepTime := authRequestTimeout + time.Second

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sh := secret

		reqAcc := strings.TrimPrefix(r.URL.EscapedPath(), "/")
		switch reqAcc {
		case waitAccID:
			time.Sleep(sleepTime)
		case unauthorizedAccID:
			sh = uuid.NewString()
		case nonExistentAccID:
			w.WriteHeader(http.StatusUnauthorized)
			_, err := w.Write([]byte(""))
			require.NoError(t, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		acc := sdnmessage.Account{
			AccountInfo: sdnmessage.AccountInfo{
				AccountID: types.AccountID(reqAcc),
				TierName:  sdnmessage.ATierUltra,
			},
			SecretHash: sh,
		}
		err := json.NewEncoder(w).Encode(&acc)
		require.NoError(t, err)
	}))
	defer authServer.Close()

	a := Auth{
		log:                  testLog,
		client:               authServer.Client(),
		authSDNUrl:           authServer.URL + "/",
		cacheIDToAccountInfo: cache.New(time.Minute*5, time.Minute*5),
	}

	svr := httptest.NewServer(alice.New(a.authMiddleware).
		ThenFunc(func(w http.ResponseWriter, r *http.Request) {
			authInfo, ok := r.Context().Value(authInfoKey).(authInfo)
			require.True(t, ok)
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(fmt.Sprintf(`{"tier":"%s"}`, authInfo[accountTier])))
			require.NoError(t, err)
		}))
	defer svr.Close()

	tests := []struct {
		name             string
		accID            string
		withWait         bool
		withUnauthorized bool
		wantTier         sdnmessage.AccountTier
	}{
		{
			name:     "happy path",
			accID:    accID,
			wantTier: sdnmessage.ATierUltra,
		},
		{
			name:     "happy path with timeout",
			accID:    waitAccID,
			withWait: true,
			wantTier: sdnmessage.ATierElite,
		},
		{
			name:             "invalid secret",
			accID:            unauthorizedAccID,
			withUnauthorized: true,
		},
		{
			name:             "non-existing acc",
			accID:            nonExistentAccID,
			withUnauthorized: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, svr.URL, nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", tt.accID, secret))))

			resp, err := svr.Client().Do(req)
			require.NoError(t, err)
			if tt.withUnauthorized {
				require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
				return
			} else {
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}
			if tt.withWait {
				_, found := a.cacheIDToAccountInfo.Get(tt.accID)
				require.False(t, found)
			}

			var m map[string]string
			err = json.NewDecoder(resp.Body).Decode(&m)
			require.NoError(t, err)
			require.Equal(t, tt.wantTier, sdnmessage.AccountTier(m["tier"]))
			require.NoError(t, resp.Body.Close())

			if tt.withWait {
				ticker := time.Tick(time.Second)
				timer := time.NewTimer(sleepTime + time.Second*5)

				var found bool

			loop:
				for {
					select {
					case <-ticker:
						_, found = a.cacheIDToAccountInfo.Get(tt.accID)
						if found {
							break loop
						}
					case <-timer.C:
						break loop
					}
				}

				require.True(t, found)
			}

			// clear the cache
			a.cacheIDToAccountInfo.Delete(tt.accID)
		})
	}
}
