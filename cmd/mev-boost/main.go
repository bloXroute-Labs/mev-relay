package main

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bloXroute-Labs/mev-relay/database"

	"github.com/bloXroute-Labs/mev-relay/server"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/sirupsen/logrus"
	blst "github.com/supranational/blst/bindings/go"
)

const (
	genesisForkVersionMainnet = "0x00000000"
	genesisForkVersionKiln    = "0x70000069"
	genesisForkVersionRopsten = "0x80000069"
	genesisForkVersionSepolia = "0x90000069"
)

var (
	version = "dev" // is set during build process

	// defaults
	defaultLogJSON            = os.Getenv("LOG_JSON") != ""
	defaultListenAddr         = getEnv("BOOST_LISTEN_ADDR", "localhost:18550")
	defaultRelayTimeoutMs     = getEnvInt("RELAY_TIMEOUT_MS", 2000) // timeout for all the requests to the relay
	defaultRelayCheck         = os.Getenv("RELAY_STARTUP_CHECK") != ""
	defaultIsRelay            = os.Getenv("IS_RELAY") != ""
	defaultGenesisForkVersion = getEnv("GENESIS_FORK_VERSION", "")
	maxHeaderBytes            = getEnvInt("MAX_HEADER_BYTES", 4000) // max header byte size for requests for dos prevention

	listenAddr     = flag.String("addr", defaultListenAddr, "listen-address for server")
	relayURLs      = flag.String("relays", "", "relay urls - single entry or comma-separated list (scheme://pubkey@host)")
	relayTimeoutMs = flag.Int("request-timeout", defaultRelayTimeoutMs, "timeout for requests to a relay [ms]")
	relayCheck     = flag.Bool("relay-check", defaultRelayCheck, "check relay status on startup and on the status API call")

	isRelay   = flag.Bool("is-relay", defaultIsRelay, "run as relay and re-sign GetHeader response")
	secretKey = flag.String("secret-key", "", "private key used for signing messages")

	// logging flags
	logJSON          = flag.Bool("json", defaultLogJSON, "log in JSON format instead of text")
	consoleLevelFlag = flag.String("log-level", "info", "log level for stdout")

	//
	nodeID                       = flag.String("node-id", "mev-boost-relay-node-id", "instance id for fluentd")
	beaconNode                   = flag.String("beacon-node", "http://localhost:5052", "url of the running beacon node")
	executionNode                = flag.String("execution-node", "http://localhost:8545", "url of the running execution node")
	beaconChain                  = flag.String("beacon-chain-url", "https://beaconcha.in", "url of the beacon chain for current network")
	etherscan                    = flag.String("etherscan-url", "https://etherscan.io", "url of etherscan for current network")
	allowedNonValidators         = flag.String("allowed-non-validators", "", "comma separate list of validator pubkeys to assume known")
	checkKnownValidatorsDisabled = flag.Bool("disable-known-validator-check", false, "disables check of known validators")
	bellatrixForkVersion         = flag.String("bellatrix-fork-version", "0x00000000", "bellatrix fork version to display on the landing page")
	genesisValidatorRoot         = flag.String("genesis-validators-root", "0x44f1e56283ca88b35c789f7f449e52339bc1fefe3a45913a43a6d16edcd33cf1", "genesis validator root to use for signing domains and landing page")
	builderIPs                   = flag.String("builder-ips", "127.0.0.1", "csv of allowed builder ips to submit payloads")
	dbHost                       = flag.String("database", "user=postgres password=password host=127.0.0.1 sslmode=disable", "database connection string")
	redisURI                     = flag.String("redis", ":6379", "redis connection uri")
	redisPrefix                  = flag.String("redis-prefix", "mev-boost-relay", "redis prefix string")
	redisPoolSize                = flag.Int("redis-pool-size", 80, "redis pool size")
	sdnURL                       = flag.String("sdn-url", "", "our sdn api url")
	cetificatesPath              = flag.String("certificates-path", "", "bdn api certificates path")
	simulationNodes              = flag.String("simulation-nodes", "", "URLs for geth nodes running simulation")
	cloudServicesEndpoint        = flag.String("cloud-services-endpoint", "", "cloud services address for sending rpc messages")
	cloudServicesAuthHeader      = flag.String("cloud-services-auth-header", "", "authorization header to use when sending rpc messages to cloud services")
	sendSlotProposerDuties       = flag.Bool("send-slot-proposer-duties", false, "send slot proposer duties rpc messages to cloud services")

	// helpers
	useGenesisForkVersionMainnet = flag.Bool("mainnet", false, "use Mainnet genesis fork version 0x00000000 (for signature validation)")
	useGenesisForkVersionKiln    = flag.Bool("kiln", false, "use Kiln genesis fork version 0x70000069 (for signature validation)")
	useGenesisForkVersionRopsten = flag.Bool("ropsten", false, "use Ropsten genesis fork version 0x80000069 (for signature validation)")
	useGenesisForkVersionSepolia = flag.Bool("sepolia", false, "use Sepolia genesis fork version 0x90000069 (for signature validation)")
	useCustomGenesisForkVersion  = flag.String("genesis-fork-version", defaultGenesisForkVersion, "use a custom genesis fork version (for signature validation)")
)

var log = logrus.WithField("module", "relay")

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flag.Parse()

	// Set the server version
	server.Version = version

	if *logJSON {
		log.Logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		log.Logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		})

	}

	if *consoleLevelFlag != "" {
		lvl, err := logrus.ParseLevel(*consoleLevelFlag)
		if err != nil {
			log.Fatalf("Invalid loglevel: %s", *consoleLevelFlag)
		}
		logrus.SetLevel(lvl)
	}

	genesisForkVersionHex := ""
	if *useCustomGenesisForkVersion != "" {
		genesisForkVersionHex = *useCustomGenesisForkVersion
	} else if *useGenesisForkVersionMainnet {
		genesisForkVersionHex = genesisForkVersionMainnet
	} else if *useGenesisForkVersionKiln {
		genesisForkVersionHex = genesisForkVersionKiln
	} else if *useGenesisForkVersionRopsten {
		genesisForkVersionHex = genesisForkVersionRopsten
	} else if *useGenesisForkVersionSepolia {
		genesisForkVersionHex = genesisForkVersionSepolia
	} else {
		log.Fatal("Please specify a genesis fork version (eg. -mainnet or -kiln or -ropsten or -genesis-fork-version flags)")
	}
	log.Infof("Using genesis fork version: %s", genesisForkVersionHex)

	relays := parseRelayURLs(*relayURLs)
	if len(relays) == 0 {
		log.Fatal("No relays specified")
	}
	log.WithField("relays", relays).Infof("using %d relays", len(relays))

	relayTimeout := time.Duration(*relayTimeoutMs) * time.Millisecond
	if relayTimeout <= 0 {
		log.Fatal("Please specify a relay timeout greater than 0")
	}

	boostSecretKey := blst.SecretKey{}
	pubKey := types.PublicKey{}
	if *isRelay {

		if *secretKey == "" {
			newPrivateKey, _, err := bls.GenerateNewKeypair()
			if err != nil {
				log.Fatal("could not generate secret key")
			}
			boostSecretKey = *newPrivateKey
			pkBytes := bls.PublicKeyFromSecretKey(&boostSecretKey).Compress()
			pubKey.FromSlice(pkBytes)
			log.Info("private key ", common.BytesToHash(boostSecretKey.ToBEndian()).Hex())
			log.Info("public key ", pubKey.String())
		} else {
			envSkBytes, err := hexutil.Decode(*secretKey)
			if err != nil {
				log.Fatal("could not decode secret key")
			}
			sk, err := bls.SecretKeyFromBytes(envSkBytes)
			if err != nil {
				log.Fatal("could not decode secret key")
			}
			boostSecretKey = *sk
			pkBytes := bls.PublicKeyFromSecretKey(&boostSecretKey).Compress()
			pubKey.FromSlice(pkBytes)
			log.Info("public key ", pubKey.String())
		}

	}

	if simulationNodes == nil || *simulationNodes == "" {
		log.Fatal("simulation IP is required")
	}

	executionNodeURL, err := url.Parse(*executionNode)
	if err != nil {
		log.WithField("execution-node", executionNode).Fatal("could not parse execution-node")
	}

	beaconChainURL, err := url.Parse(*beaconChain)
	if err != nil {
		log.WithError(err).Fatal("could not parse beacon-chain-url")
	}
	etherscanURL, err := url.Parse(*etherscan)
	if err != nil {
		log.WithError(err).Fatal("could not parse etherscan-url")
	}

	db, err := database.NewDatabaseService(*dbHost)
	if err != nil {
		log.Error("could not connect to db, ", "error: ", err)
	}

	opts := server.RelayServiceOpts{
		Log:                     log,
		ListenAddr:              *listenAddr,
		Relays:                  relays,
		GenesisForkVersionHex:   genesisForkVersionHex,
		RelayRequestTimeout:     relayTimeout,
		RelayCheck:              *relayCheck,
		MaxHeaderBytes:          maxHeaderBytes,
		IsRelay:                 *isRelay,
		SecretKey:               &boostSecretKey,
		PubKey:                  pubKey,
		BeaconNode:              *beaconNode,
		ExecutionNode:           *executionNodeURL,
		BeaconChain:             *beaconChainURL,
		Etherscan:               *etherscanURL,
		KnownValidators:         *allowedNonValidators,
		CheckKnownValidators:    !*checkKnownValidatorsDisabled,
		BellatrixForkVersionHex: *bellatrixForkVersion,
		GenesisValidatorRootHex: *genesisValidatorRoot,
		BuilderIPs:              *builderIPs,
		DB:                      db,
		RedisURI:                *redisURI,
		RedisPrefix:             *redisPrefix,
		SDNURL:                  *sdnURL,
		CertificatesPath:        *cetificatesPath,
		SimulationNodes:         *simulationNodes,
		CloudServicesEndpoint:   *cloudServicesEndpoint,
		CloudServicesAuthHeader: *cloudServicesAuthHeader,
		SendSlotProposerDuties:  *sendSlotProposerDuties,
		RelayType:               getRelayType(*nodeID, *isRelay),
		RedisPoolSize:           *redisPoolSize,
	}
	server, err := server.NewRelayService(opts)
	if err != nil {
		log.WithError(err).Fatal("failed creating the server")
	}

	go func() {
		log.Info("pprof http server is running on 0.0.0.0:6060")
		log.Info(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	// TODO: add ctx to all go routine so we can exit nicely
	go server.StartFetchValidators()
	go server.StartActivityLogger(ctx)
	go server.SubscribeToBlocks()

	if *relayCheck && !server.CheckRelays() {
		log.Fatal("no relay available")
	}

	log.Println("listening on", *listenAddr)
	log.Fatal(server.StartHTTPServer())
}

func getEnv(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value, ok := os.LookupEnv(key); ok {
		val, err := strconv.Atoi(value)
		if err == nil {
			return val
		}
	}
	return defaultValue
}

func parseRelayURLs(relayURLs string) []server.RelayEntry {
	ret := []server.RelayEntry{}
	for _, entry := range strings.Split(relayURLs, ",") {
		relay, err := server.NewRelayEntry(entry)
		if err != nil {
			log.WithError(err).WithField("relayURL", entry).Fatal("Invalid relay URL")
		}
		ret = append(ret, relay)
	}
	return ret
}

func getRelayType(nodeID string, isRelay bool) server.RelayType {
	if strings.Contains(nodeID, "maxprofit") {
		return server.RelayMaxProfit
	}
	if strings.Contains(nodeID, "ethical") {
		return server.RelayEthical
	}
	if strings.Contains(nodeID, "regulated") {
		return server.RelayRegulated
	}
	if isRelay {
		log.Error("relay type could not be extracted from node ID")
	}
	return server.RelayUnknown
}
