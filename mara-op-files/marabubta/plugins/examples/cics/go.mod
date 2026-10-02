module github.com/marabunta/marabunta-cics

go 1.21

require (
	github.com/marabunta/swarm-plugin-go v0.0.0
	github.com/tetratelabs/wazero v1.6.0
)

replace github.com/marabunta/swarm-plugin-go => ../shared/go/swarmclient
