package worker

import "go.uber.org/fx"

var Provider = fx.Options(
	fx.Provide(NewWorkerDataLayer),
)
