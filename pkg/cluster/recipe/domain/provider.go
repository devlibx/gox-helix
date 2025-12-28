package domain

import "go.uber.org/fx"

var Provider = fx.Options(
	fx.Provide(NewService),
	fx.Provide(NewDomainDataLayer),
)
