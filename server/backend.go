package server

// Version is used to get the version of the backend. It is set by the cli
var Version = "dev"

// Router paths
var (
	pathStatus            = "/eth/v1/builder/status"
	pathRegisterValidator = "/eth/v1/builder/validators"
	pathGetHeaderPrefix   = "/eth/v1/builder/header"
	pathGetHeader         = "/eth/v1/builder/header/{slot:[0-9]+}/{parent_hash:0x[a-fA-F0-9]+}/{pubkey:0x[a-fA-F0-9]+}"
	pathGetPayload        = "/eth/v1/builder/blinded_blocks"
	pathPutRelay          = "/relays"

	pathBuilderGetValidators = "/relay/v1/builder/validators"
	pathSubmitNewBlock       = "/relay/v1/builder/blocks"

	pathDataProposerPayloadDelivered = "/relay/v1/data/bidtraces/proposer_payload_delivered"
	pathDataBuilderBidsReceived      = "/relay/v1/data/bidtraces/builder_blocks_received"
	pathDataValidatorRegistration    = "/relay/v1/data/validator_registration"

	pathBuilderDeleteBlocks             = "/blxr/v1/builder/delete_blocks"
	pathBuilderDisableGetHeaderResponse = "/blxr/v1/builder/disable_get_header_response"

	pathGetActiveValidators = "/blxr/v1/validators/active"

	pathWebsocket = "/blxr/ws"
)
