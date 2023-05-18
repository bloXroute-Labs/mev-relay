package server

import (
	"context"
	"errors"
	"html/template"
	"net/http"
	"os"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func (m *BoostService) handleIndex(w http.ResponseWriter, req *http.Request) {
	t, err := indexTemplate()
	if err != nil {
		m.respondErrorWithLog(w, http.StatusInternalServerError, "Oops! Something went wrong.", m.log.WithError(err), "could not read index html for home page")
		return
	}

	// Do we need a lock just to check length?
	m.validatorsLock.RLock()
	noValidators := len(m.validators.Keys())
	m.validatorsLock.RUnlock()

	activeValidatorsCount, err := m.datastore.GetActiveValidators(req.Context())
	if err != nil {
		// no need to process this request if the context is cancelled
		if errors.Is(err, context.Canceled) {
			return
		}
		activeValidatorsCount = 0
		m.log.WithError(err).Error("could not fetch active validators for home page")
	}

	statusData := struct {
		Pubkey                string
		NoValidators          int
		ActiveValidators      int
		GenesisForkVersion    string
		BellatrixForkVersion  string
		GenesisValidatorsRoot string
		BuilderSigningDomain  string
		ProposerSigningDomain string
	}{hexutil.Encode(m.pubKey[:]), noValidators, activeValidatorsCount, m.genesisForkVersion, m.bellatrixForkVersion, m.genesisValidatorRootHex, hexutil.Encode(m.builderSigningDomain[:]), hexutil.Encode(m.proposerSigningDomain[:])}

	if err := t.Execute(w, statusData); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func indexTemplate() (*template.Template, error) {
	data, err := os.ReadFile("./static/index.html")
	if err != nil {
		return template.New("index").Parse("")
	}
	return template.New("index").Parse(string(data))
}
