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
	"github.com/bloXroute-Labs/gateway/v2/types"
	"github.com/bloXroute-Labs/mev-relay/common"
	"github.com/cenkalti/backoff/v4"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const (
	maxConnsPerHost = 5000
	// how much time should middleware wait for the account response
	authRequestTimeout = time.Second * 5
	// how much time a process tries to get account info
	authRequestMaxAttemptTime = time.Minute * 2
)

var (
	errNoPermission       = errors.New("your account does not have permissions to use this endpoint")
	errWrongNumberOfParts = errors.New("wrong number of parts")
	errWrongHash          = errors.New("id/secret hash is wrong")
	errMissingAuthHeader  = errors.New("missing auth header")
	errInvalidAuthHeader  = "invalid auth header"
)

type Auth struct {
	log        *logrus.Entry
	client     *http.Client
	authSDNUrl string
	builderIPs string

	cacheIDToAccountInfo *cache.Cache
}

func NewAuth(log *logrus.Entry, certificatesPath, sdnURL, builderIPs string) Auth {
	auth := Auth{
		log:                  log.WithField("middleware", "auth"),
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

func (a Auth) whitelistIPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			log := a.log
			clientIPAddress := common.GetIPXForwardedFor(r)
			log.WithField("clientIPAddress", clientIPAddress).WithField("method", "deleteBlocks")
			if checkThatRequestPassedAuthorization(r.Context()) {
				next.ServeHTTP(w, r)
				return
			}

			whitelistIP := strings.Contains(a.builderIPs, strings.Split(r.RemoteAddr, ":")[0]) || strings.Contains(a.builderIPs, clientIPAddress)
			if whitelistIP {
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authInfoKey, newAuthInfo(whitelistIPAuth))))
				return
			}

			if a.authSDNUrl == "" {
				a.log.WithField("remote-address", r.RemoteAddr).Error("submit payload from unknown address")
				respondError(log, w, http.StatusUnauthorized, "unauthorized")
				return
			}

			next.ServeHTTP(w, r)
		},
	)
}

func (a Auth) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			log := a.log
			clientIPAddress := common.GetIPXForwardedFor(r)
			log.WithField("clientIPAddress", clientIPAddress).WithField("method", "deleteBlocks")
			if checkThatRequestPassedAuthorization(r.Context()) {
				next.ServeHTTP(w, r)
				return
			}

			accountID, secret, err := a.parseAuthHeader(r.Header.Get("Authorization"))
			if err != nil {
				log.Errorf("remote_addr=%v request_uri=%v, failed to parse auth header: %v",
					r.RemoteAddr, r.RequestURI, err)
				respondError(log, w, http.StatusUnauthorized, errInvalidAuthHeader)
				return
			}

			var builderAccount sdnmessage.Account

			account, found := a.cacheIDToAccountInfo.Get(accountID)
			if found {
				builderAccount = account.(sdnmessage.Account)
			} else {
				builderAccount, err = a.processAccountRequest(accountID, secret)
				if err != nil {
					message := "can not authorize account"
					if errors.Is(err, errNoPermission) {
						message = errNoPermission.Error()
					}
					respondError(log, w, http.StatusUnauthorized, message)
					return
				}
			}

			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authInfoKey,
				newAuthInfo(headerAuth).addAccountID(accountID).addAccountTier(builderAccount.TierName))))
		},
	)
}

func (a Auth) parseAuthHeader(authHeader string) (accountID string, secret string, err error) {
	if authHeader == "" {
		return "", "", errMissingAuthHeader
	}
	payload, err := base64.StdEncoding.DecodeString(authHeader)
	if err != nil {
		return "", "", err
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 {
		return "", "", errWrongNumberOfParts
	}

	return parts[0], parts[1], nil
}

func (a Auth) processAccountRequest(accountID, secret string) (sdnmessage.Account, error) {
	authResponse := make(chan asyncAuthResponse)

	// get account info on the background
	go a.getAccount(accountID, authResponse)

	var auth asyncAuthResponse
	var gotResponse bool

	// wait for the response or timeout
	select {
	case <-time.NewTimer(authRequestTimeout).C:
		// wait for the result on the background
		go a.waitAndCache(authResponse, accountID, secret)
	case auth, gotResponse = <-authResponse:
	}

	// if no response received, make this account elite, but don't add to the cache
	if !gotResponse {
		a.log.Warnf("no response received from auth server within %v, assuming account elite", authRequestTimeout)
		auth.acc.TierName = sdnmessage.ATierElite
		auth.acc.AccountID = types.AccountID(accountID)
		return auth.acc, nil
	}

	// auth server returned an error
	if auth.err != nil {
		a.log.Errorf("failed to get account '%s': %v", accountID, auth.err)
		return sdnmessage.Account{}, auth.err
	}

	// validate acc secret
	err := a.validateAccount(auth.acc, secret)
	if err != nil {
		a.log.Errorf("failed to validate account %s: %v", accountID, err)
		return sdnmessage.Account{}, errNoPermission
	}

	a.log.Infof("saving account %s to cache after receiving response from the sdn", auth.acc.AccountID)
	a.cacheIDToAccountInfo.Set(string(auth.acc.AccountID), auth.acc, time.Hour)
	return auth.acc, nil
}

func (a Auth) getAccount(accountID string, accResp chan asyncAuthResponse) {
	defer close(accResp)

	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = authRequestMaxAttemptTime

	err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*authRequestTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.authSDNUrl+accountID, nil)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("failed to create auth request: %w", err))
		}
		res, err := a.client.Do(req)
		if err != nil {
			// this is the only non-permanent error
			return fmt.Errorf("account request to check authorization failed: %w", err)
		}
		defer func(r io.ReadCloser) {
			err = r.Close()
			if err != nil {
				a.log.Warnf("failed to close request body of account '%s': %v", accountID, err)
			}
		}(res.Body)

		if res.StatusCode != http.StatusOK {
			return backoff.Permanent(fmt.Errorf("invalid authorization header, auth server response status code: %d", res.StatusCode))
		}
		var authResponse sdnmessage.Account
		err = json.NewDecoder(res.Body).Decode(&authResponse)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("could not convert body of JSON response to an authResponse struct: %w", err))
		}

		accResp <- asyncAuthResponse{acc: authResponse}
		return nil
	}, bo)

	if err != nil {
		accResp <- asyncAuthResponse{err: err}
	}
}

type asyncAuthResponse struct {
	acc sdnmessage.Account
	err error
}

// waitAndCache will wait for the auth server response and add account to the cache if it's valid
func (a Auth) waitAndCache(authResponse chan asyncAuthResponse, accountID, secret string) {
	auth, ok := <-authResponse
	if !ok {
		return
	}
	if auth.err != nil {
		a.log.WithError(auth.err).Errorf("could not get account info")
		return
	}
	err := a.validateAccount(auth.acc, secret)
	if err != nil {
		a.log.Errorf("failed to validate account %s: %v", accountID, err.Error())
		return
	}
	a.log.Infof("saving account %s to cache after receiving response from the sdn", auth.acc.AccountID)
	a.cacheIDToAccountInfo.Set(string(auth.acc.AccountID), auth.acc, time.Hour)
}

func (a Auth) validateAccount(account sdnmessage.Account, accountSecret string) error {
	if account.SecretHash != accountSecret {
		return errWrongHash
	}

	if !account.TierName.IsEnterprise() {
		return fmt.Errorf("account %s does not have permissions to use this endpoint, account tier: %s", account.AccountID, account.TierName)
	}

	return nil
}

// TODO: make it generic
func respondError(log *logrus.Entry, w http.ResponseWriter, code int, message string) {
	log.WithFields(logrus.Fields{"error": message, "code": code}).Error("responding to client with error")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := httpErrorResp{code, message}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.WithField("response", resp).WithError(err).Error("couldn't write error response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (a Auth) rpcAuthentication(ctx context.Context) (context.Context, error) {

	p, _ := peer.FromContext(ctx)
	clientIPAddress := p.Addr.String()

	whitelistIP := strings.Contains(a.builderIPs, strings.Split(clientIPAddress, ":")[0]) || strings.Contains(a.builderIPs, clientIPAddress)
	if whitelistIP {
		return ctx, nil
	}

	auth, err := ReadAuthMetadata(ctx)
	if err != nil {
		return ctx, err
	}

	var builderAccount sdnmessage.Account

	accountID, secret, err := a.parseAuthHeader(auth)
	if err != nil {
		a.log.Info("failed to parse RPC Auth Header")
		return ctx, err
	}

	account, found := a.cacheIDToAccountInfo.Get(accountID)
	if found {
		builderAccount = account.(sdnmessage.Account)
	} else {
		builderAccount, err = a.processAccountRequest(accountID, secret)
		if err != nil {
			return ctx, err
		}
	}

	return context.WithValue(ctx, authInfoKey,
		newAuthInfo(headerAuth).addAccountID(accountID).addAccountTier(builderAccount.TierName)), nil
}

// ReadAuthMetadata reads auth info from the RPC connection context
func ReadAuthMetadata(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("could not read metadata from context")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return "", errors.New("no auth information was provided")
	}
	return values[0], nil
}
