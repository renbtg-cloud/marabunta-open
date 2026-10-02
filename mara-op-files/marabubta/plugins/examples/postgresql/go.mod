module github.com/marabunta/marabunta-postgres

go 1.21

require (
	github.com/marabunta/swarm-plugin-go v0.0.0
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/pganalyze/pg_query_go/v5 v5.1.0
)

require google.golang.org/protobuf v1.31.0 // indirect

replace github.com/marabunta/swarm-plugin-go => ../shared/go/swarmclient
