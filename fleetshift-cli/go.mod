module github.com/fleetshift/fleetshift-poc/fleetshift-cli

go 1.25.0

require (
	github.com/fleetshift/fleetshift-poc/fleetshift-server v0.0.0
	github.com/spf13/cobra v1.10.2
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
)

replace github.com/fleetshift/fleetshift-poc/fleetshift-server => ../fleetshift-server
