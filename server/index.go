package server

import (
	"html/template"
	"net/http"
	"os"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func (m *RelayService) handleIndex(w http.ResponseWriter, req *http.Request) {
	t, err := indexTemplate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	noValidators := len(m.validators)

	statusData := struct {
		Pubkey                string
		NoValidators          int
		GenesisForkVersion    string
		BellatrixForkVersion  string
		GenesisValidatorsRoot string
		BuilderSigningDomain  string
		ProposerSigningDomain string
	}{hexutil.Encode(m.pubKey[:]), noValidators, m.genesisForkVersion, m.bellatrixForkVersion, m.genesisValidatorRootHex, hexutil.Encode(m.builderSigningDomain[:]), hexutil.Encode(m.proposerSigningDomain[:])}

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
