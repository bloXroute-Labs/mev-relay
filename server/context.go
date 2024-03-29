package server

import (
	"context"

	"github.com/bloXroute-Labs/gateway/v2/sdnmessage"
)

type authContextKey string

const (
	authInfoKey authContextKey = "authInfoKey"

	passed       authContextKey = "passed"
	authorizedBy authContextKey = "authorizedBy"

	whitelistIPAuth authContextKey = "whitelistIP"
	headerAuth      authContextKey = "header"

	accountIDKey authContextKey = "accountID"

	accountTier authContextKey = "accountTier"
)

type authInfo map[authContextKey]any

func (a authInfo) addAccountID(accountID string) authInfo {
	a[accountIDKey] = accountID
	return a
}

func (a authInfo) addAccountTier(accTierValue sdnmessage.AccountTier) authInfo {
	a[accountTier] = accTierValue
	return a
}

func newAuthInfo(authorizedByMiddleware authContextKey) authInfo {
	return authInfo{passed: struct{}{}, authorizedBy: authorizedByMiddleware}
}

func checkThatRequestPassedAuthorization(ctx context.Context) bool {
	authInfo, ok := ctx.Value(authInfoKey).(authInfo)
	if !ok {
		return false
	}

	_, ok = authInfo[passed]

	return ok
}

func checkThatRequestAuthorizedBy(ctx context.Context, key authContextKey) bool {
	authInfo, ok := ctx.Value(authInfoKey).(authInfo)
	if !ok {
		return false
	}

	if authInfo[authorizedBy] == key {
		return true
	}

	return false
}

func getInfoFromRequest(ctx context.Context, key authContextKey) any {
	return ctx.Value(authInfoKey).(authInfo)[key]
}
