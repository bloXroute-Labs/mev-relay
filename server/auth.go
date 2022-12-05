package server

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

const maxConnsPerHost = 5000

type Auth struct {
	log        *logrus.Entry
	client     *http.Client
	authSDNUrl string
	builderIPs string

	cacheIDToAccountInfo *cache.Cache
}

func NewAuth(log *logrus.Entry, certificatesPath, sdnURL, builderIPs string) Auth {
	auth := Auth{
		log:                  log,
		builderIPs:           builderIPs,
		cacheIDToAccountInfo: cache.New(time.Hour, time.Hour),
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		MaxConnsPerHost:   maxConnsPerHost,
		DisableKeepAlives: true,
	}

	if sdnURL != "" {
		auth.authSDNUrl = fmt.Sprintf("%s/accounts/", sdnURL)
		if certificatesPath != "" {
			keyPair, err := common.CreateKeyPair(certificatesPath)
			if err != nil {
				log.Fatal("failed to load api certificates", err.Error())
			}

			transport.TLSClientConfig.Certificates = []tls.Certificate{keyPair}
		} else {
			log.Fatal("certificates-path and sdn-url must be provided")
		}
	}

	auth.client = &http.Client{
		Transport: transport,
	}

	return auth
}

func (a Auth) whitelistIPMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if checkThatRequestPassedAuthorization(r.Context()) {
			next.ServeHTTP(w, r)
			return
		}

		whitelistIP := strings.Contains(a.builderIPs, strings.Split(r.RemoteAddr, ":")[0])
		if whitelistIP {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authInfoKey, newAuthInfo(whitelistIPAuth))))
			return
		}

		if a.authSDNUrl == "" {
			a.log.WithField("remote-address", r.RemoteAddr).Warn("submit payload from unknown address")
			respondError(a.log, w, http.StatusUnauthorized, "unauthorized")
			return
		}

		next.ServeHTTP(w, r)
	}
}

func (a Auth) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if checkThatRequestPassedAuthorization(r.Context()) {
			next.ServeHTTP(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			respondError(a.log, w, http.StatusUnauthorized, "authorization header not found")
			return
		}

		accountID, secret, err := a.parseAuthHeader(authHeader)
		if err != nil {
			a.log.Errorf("RemoteAddr: %v Request URI: %v Account: %s, failed to parse auth header, %v", r.RemoteAddr, r.RequestURI, accountID, err)
			respondError(a.log, w, http.StatusUnauthorized, err.Error())
			return
		}

		var builderAccount sdnmessage.Account

		account, found := a.cacheIDToAccountInfo.Get(accountID)
		if found {
			builderAccount = account.(sdnmessage.Account)
		} else {
			builderAccount, err = a.processAccountRequest(accountID)
			if err != nil {
				a.log.Errorf("failed to process account %s request %v", accountID, err)
				respondError(a.log, w, http.StatusUnauthorized, "can not authorize account")
				return
			}
		}

		err = a.validateAccount(builderAccount, secret)
		if err != nil {
			a.log.Infof("failed to validate account %s, %s", accountID, err.Error())
			respondError(a.log, w, http.StatusUnauthorized, "your account does not have permissions to use this endpoint")
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authInfoKey, newAuthInfo(headerAuth).addAccountID(accountID))))
	}
}

func (a Auth) parseAuthHeader(authHeader string) (accountID string, secret string, err error) {
	payload, err := base64.StdEncoding.DecodeString(authHeader)
	if err != nil {
		return "", "", fmt.Errorf("could not decode auth header: %v", authHeader)
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("could not decode auth header: %v", authHeader)
	}

	return parts[0], parts[1], nil
}

func (a Auth) processAccountRequest(accountID string) (sdnmessage.Account, error) {
	res, err := a.client.Get(a.authSDNUrl + accountID)
	if err != nil {
		return sdnmessage.Account{}, fmt.Errorf("account request to check authorization failed: %s", err.Error())
	}

	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			a.log.Warnf("body of account %s request to check authorization not closed properly: %v", accountID, err.Error())
		}
	}(res.Body)

	if res.StatusCode != 200 {
		return sdnmessage.Account{}, fmt.Errorf("the parsed account id/secret hash is wrong; invalid authorization header, status code: %d", res.StatusCode)
	}

	authResponse := sdnmessage.Account{}
	err = json.NewDecoder(res.Body).Decode(&authResponse)
	if err != nil {
		return sdnmessage.Account{}, fmt.Errorf("could not convert body of JSON response to an authResponse struct: %v", err)
	}

	a.cacheIDToAccountInfo.Set(string(authResponse.AccountID), authResponse, time.Hour)

	return authResponse, nil
}

func (a Auth) validateAccount(account sdnmessage.Account, accountSecret string) error {
	if account.SecretHash != accountSecret {
		return errors.New("the parsed account id/secret hash is wrong; invalid authorization header")
	}

	if !account.TierName.IsElite() {
		return fmt.Errorf("account %s does not have permissions to use this endpoint, account tier: %s", account.AccountID, account.TierName)
	}

	return nil
}

// TODO: make it generic
func respondError(log *logrus.Entry, w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := httpErrorResp{code, message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.WithField("response", resp).WithError(err).Error("Couldn't write error response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}
