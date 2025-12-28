package locker

import "go.uber.org/fx"

var Provider = fx.Options(
	fx.Provide(NewLockerDataLayer), // Locker data layer
	fx.Provide(NewLocker),          // Locker

)
