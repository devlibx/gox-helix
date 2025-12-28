package common

import "context"

type ApplicationStopSignal struct {
	Ctx           context.Context
	ContextCancel context.CancelFunc
}
