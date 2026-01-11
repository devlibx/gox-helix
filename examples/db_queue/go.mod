module github.com/devlibx/gox-helix/examples/db_queue

go 1.23.1

replace github.com/devlibx/gox-helix => ../..

require (
	github.com/devlibx/gox-base/v2 v2.0.39
	github.com/devlibx/gox-helix v0.0.0-00010101000000-000000000000
	github.com/go-sql-driver/mysql v1.9.3
	github.com/oklog/ulid/v2 v2.1.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	go.uber.org/fx v1.24.0
	go.uber.org/ratelimit v0.3.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/fatih/structs v1.1.0 // indirect
	github.com/google/uuid v1.2.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.uber.org/dig v1.19.0 // indirect
	go.uber.org/mock v0.5.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
)
