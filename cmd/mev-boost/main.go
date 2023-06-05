package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bloXroute-Labs/gateway/v2/utils/syncmap"
	"github.com/bloXroute-Labs/mev-relay/database"
	"github.com/bloXroute-Labs/mev-relay/server"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	blst "github.com/supranational/blst/bindings/go"
	"github.com/uptrace/uptrace-go/uptrace"
	"go.opentelemetry.io/otel"
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

	// cli flags
	printVersion = flag.Bool("version", false, "only print version")

	listenAddr     = flag.String("addr", defaultListenAddr, "listen-address for mev-boost server")
	relayURLs      = flag.String("relays", "", "relay urls - single entry or comma-separated list (scheme://pubkey@host)")
	relayTimeoutMs = flag.Int("request-timeout", defaultRelayTimeoutMs, "timeout for requests to a relay [ms]")
	relayCheck     = flag.Bool("relay-check", defaultRelayCheck, "check relay status on startup and on the status API call")

	isRelay        = flag.Bool("is-relay", defaultIsRelay, "run as relay and re-sign GetHeader response")
	secretKey      = flag.String("secret-key", "", "private key used for signing messages")
	fluentdEnabled = flag.Bool("fluentd", false, "should fluentd run")

	// logging flags
	logJSON           = flag.Bool("json", defaultLogJSON, "log in JSON format instead of text")
	consoleLevelFlag  = flag.String("log-level", "info", "log level for stdout")
	fileLevelFlag     = flag.String("log-file-level", "trace", "log level for the log file")
	fluentdHost       = flag.String("fluentd-host", "http://localhost", "fluentd ip")
	logMaxSizeFlag    = flag.Int("log-max-size", 100, "maximum size in megabytes of the log file before it gets rotated")
	logMaxAgeFlag     = flag.Int("log-max-age", 10, "maximum number of days to retain old log files based on the timestamp encoded in their filename")
	logMaxBackupsFlag = flag.Int("log-max-backups", 10, "maximum number of old log files to retain")

	//
	nodeID                       = flag.String("node-id", "mev-boost-relay-node-id", "instance id for fluentd")
	externalIP                   = flag.String("external-ip", "", "external ip")
	beaconNode                   = flag.String("beacon-node", "http://localhost:5052", "url of the running beacon node")
	executionNode                = flag.String("execution-node", "http://localhost:8545", "url of the running execution node")
	beaconChain                  = flag.String("beacon-chain-url", "https://beaconcha.in", "url of the beacon chain for current network")
	etherscan                    = flag.String("etherscan-url", "https://etherscan.io", "url of etherscan for current network")
	allowedNonValidators         = flag.String("allowed-non-validators", "", "comma separate list of validator pubkeys to assume known")
	checkKnownValidatorsDisabled = flag.Bool("disable-known-validator-check", false, "disables check of known validators")
	bellatrixForkVersion         = flag.String("bellatrix-fork-version", "0x00000000", "bellatrix fork version to display on the landing page")
	capellaForkVersion           = flag.String("capella-fork-version", "0x00000000", "capella fork version to display on the landing page")
	genesisValidatorRoot         = flag.String("genesis-validators-root", "0x44f1e56283ca88b35c789f7f449e52339bc1fefe3a45913a43a6d16edcd33cf1", "genesis validator root to use for signing domains and landing page")
	builderIPs                   = flag.String("builder-ips", "127.0.0.1", "csv of allowed builder ips to submit payloads")
	highPriorityBuilderPubkeys   = flag.String("high-priority-builder-pubkeys", "", "csv of builder public keys whose blocks will skip simulation if value if below a set amount")
	highPerfSimBuilderPubkeys    = flag.String("high-perf-sim-builder-pubkeys", "", "csv of builder public keys that are allowed to use simulation-nodes-high-perf for block simulation")
	dbHost                       = flag.String("database", "user=postgres password=password host=127.0.0.1 sslmode=disable", "database connection string")
	redisURI                     = flag.String("redis", ":6379", "redis connection uri")
	redisPrefix                  = flag.String("redis-prefix", "mev-boost-relay", "redis prefix string")
	redisPoolSize                = flag.Int("redis-pool-size", 80, "redis pool size")
	sdnURL                       = flag.String("sdn-url", "", "our sdn api url")
	cetificatesPath              = flag.String("certificates-path", "", "bdn api certificates path")
	simulationNodes              = flag.String("simulation-nodes", "", "URLs for geth nodes running simulation")
	simulationNodeHighPerf       = flag.String("simulation-node-high-perf", "", "URL for high performance simulation node, reserved for specific builder public keys")
	cloudServicesEndpoint        = flag.String("cloud-services-endpoint", "", "cloud services address for sending rpc messages")
	cloudServicesAuthHeader      = flag.String("cloud-services-auth-header", "", "authorization header to use when sending rpc messages to cloud services")
	sendSlotProposerDuties       = flag.Bool("send-slot-proposer-duties", false, "send slot proposer duties rpc messages to cloud services")
	topBlockLimit                = flag.Int("top-block-limit", 3, "number of top blocks to save per slot")
	externalRelaysForComparison  = flag.String("external-relays-for-comparison", "", "")
	getPayloadRequestCutoff      = flag.Int("getpayload-request-cutoff", 4000, "getPayload request cutoff time in ms")
	getHeaderRequestCutoff       = flag.Int("getheader-request-cutoff", 3000, "getHeader request cutoff time in ms")
	enableBidSaveCancellation    = flag.Bool("enable-bid-save-cancellation", false, "enables cancellation of saving bids due to later blocks received from the same builder")

	uptraceDSN = flag.String("uptrace-dsn", "", "url for uptrace dsn")

	capellaForkEpoch = flag.Int64("capella-fork-epoch", 0, "")

	// helpers
	useGenesisForkVersionMainnet = flag.Bool("mainnet", false, "use Mainnet genesis fork version 0x00000000 (for signature validation)")
	useGenesisForkVersionKiln    = flag.Bool("kiln", false, "use Kiln genesis fork version 0x70000069 (for signature validation)")
	useGenesisForkVersionRopsten = flag.Bool("ropsten", false, "use Ropsten genesis fork version 0x80000069 (for signature validation)")
	useGenesisForkVersionSepolia = flag.Bool("sepolia", false, "use Sepolia genesis fork version 0x90000069 (for signature validation)")
	useCustomGenesisForkVersion  = flag.String("genesis-fork-version", defaultGenesisForkVersion, "use a custom genesis fork version (for signature validation)")

	updateActiveValidators = flag.Bool("update-active-validators", false, "if this relay should manage updating active validators")
)

var log = logrus.WithField("module", "cmd/mev-boost")

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flag.Parse()

	var fluentdNodeID string
	switch {
	case *nodeID != "":
		fluentdNodeID = *nodeID
	case *externalIP != "":
		fluentdNodeID = uuid.NewMD5(uuid.NameSpaceDNS, []byte(fmt.Sprintf("%s:%s", *externalIP, *listenAddr))).String()
	default:
		uuid, err := uuid.NewUUID()
		if err != nil {
			fluentdNodeID = "Unknown-node-id_and_external-ip"
		} else {
			fluentdNodeID = uuid.String()
		}
	}

	// Configure OpenTelemetry with sensible defaults.
	uptrace.ConfigureOpentelemetry(
		uptrace.WithDSN(*uptraceDSN),

		uptrace.WithServiceName("mev-boost-relay"),
		uptrace.WithServiceVersion("1.0.0"),
		uptrace.WithDeploymentEnvironment(fluentdNodeID),
	)
	// Send buffered spans and free resources.
	defer uptrace.Shutdown(ctx)

	tracer := otel.Tracer("main")

	server.InitLogger(*consoleLevelFlag, *fileLevelFlag, *fluentdHost, fluentdNodeID, *fluentdEnabled, *logMaxSizeFlag, *logMaxAgeFlag, *logMaxBackupsFlag)

	if *printVersion {
		fmt.Printf("mev-boost %s\n", version)
		return
	}
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

	log.Infof("mev-boost %s", version)

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

	log.Infof("Connecting to Postgres database...")
	db, err := database.NewDatabaseService(*dbHost)
	if err != nil {
		log.Error("could not connect to db, ", "error: ", err)
	}
	log.Infof("Connected to Postgres database")

	highPriorityBuilderPubkeysMap := syncmap.NewStringMapOf[bool]()
	if *highPriorityBuilderPubkeys != "" {
		highPriorityBuilderPubkeysSlice := strings.Split(*highPriorityBuilderPubkeys, ",")
		for _, pubkey := range highPriorityBuilderPubkeysSlice {
			highPriorityBuilderPubkeysMap.Store(pubkey, true)
		}
	}
	highPerfSimBuilderPubkeysMap := syncmap.NewStringMapOf[bool]()
	if *highPerfSimBuilderPubkeys != "" {
		highPerfSimBuilderPubkeysSlice := strings.Split(*highPerfSimBuilderPubkeys, ",")
		for _, pubkey := range highPerfSimBuilderPubkeysSlice {
			highPerfSimBuilderPubkeysMap.Store(pubkey, true)
		}
	}

	opts := server.BoostServiceOpts{
		Log:                         log,
		ListenAddr:                  *listenAddr,
		Relays:                      relays,
		GenesisForkVersionHex:       genesisForkVersionHex,
		RelayRequestTimeout:         relayTimeout,
		RelayCheck:                  *relayCheck,
		MaxHeaderBytes:              maxHeaderBytes,
		IsRelay:                     *isRelay,
		SecretKey:                   &boostSecretKey,
		PubKey:                      pubKey,
		BeaconNode:                  *beaconNode,
		ExecutionNode:               *executionNodeURL,
		BeaconChain:                 *beaconChainURL,
		Etherscan:                   *etherscanURL,
		KnownValidators:             *allowedNonValidators,
		CheckKnownValidators:        !*checkKnownValidatorsDisabled,
		BellatrixForkVersionHex:     *bellatrixForkVersion,
		CapellaForkVersionHex:       *capellaForkVersion,
		GenesisValidatorRootHex:     *genesisValidatorRoot,
		BuilderIPs:                  *builderIPs,
		HighPriorityBuilderPubkeys:  highPriorityBuilderPubkeysMap,
		HighPerfSimBuilderPubkeys:   highPerfSimBuilderPubkeysMap,
		DB:                          db,
		RedisURI:                    *redisURI,
		RedisPrefix:                 *redisPrefix,
		SDNURL:                      *sdnURL,
		CertificatesPath:            *cetificatesPath,
		SimulationNodes:             *simulationNodes,
		SimulationNodeHighPerf:      *simulationNodeHighPerf,
		CloudServicesEndpoint:       *cloudServicesEndpoint,
		CloudServicesAuthHeader:     *cloudServicesAuthHeader,
		SendSlotProposerDuties:      *sendSlotProposerDuties,
		RelayType:                   getRelayType(*nodeID, *isRelay),
		RedisPoolSize:               *redisPoolSize,
		TopBlockLimit:               *topBlockLimit,
		ExternalRelaysForComparison: parseExternalRelaysForComparisonURLs(*externalRelaysForComparison),
		GetPayloadRequestCutoffMs:   *getPayloadRequestCutoff,
		GetHeaderRequestCutoffMs:    *getHeaderRequestCutoff,
		CapellaForkEpoch:            *capellaForkEpoch,

		Tracer: tracer,
	}
	server, err := server.NewBoostService(opts)
	if err != nil {
		log.WithError(err).Fatal("failed creating the server")
	}

	go func() {
		log.Info("pprof http server is running on 0.0.0.0:6060")
		log.Info(http.ListenAndServe("0.0.0.0:6060", nil))
	}()

	// TODO: add ctx to all go routine so we can exit nicely
	go server.StartFetchValidators()
	if *updateActiveValidators {
		go server.UpdateActiveValidators()
	}
	go server.StartActivityLogger(ctx)
	go server.SubscribeToBlocks()
	go server.SubscribeToPayloadAttributes()
	go server.StartConfigFilesLoading()
	go server.StartSendNextSlotProposerDuties()

	if *enableBidSaveCancellation {
		go server.StartSaveBlocksPubSub()
	}

	if *fluentdEnabled {
		server.StartStats(*fluentdHost, fluentdNodeID)
	}

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

func parseExternalRelaysForComparisonURLs(relayURLs string) []string {
	urls := strings.Split(relayURLs, ",")
	if len(urls) == 1 && urls[0] == "" {
		return []string{}
	}

	return urls
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
