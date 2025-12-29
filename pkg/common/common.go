package common

import (
	"context"
	"github.com/google/uuid"
	"log/slog"
	"sync"
)

type ApplicationSingleton struct {
	applicationCtx           context.Context
	applicationContextCancel context.CancelFunc
	workerId                 string
	logger                   *slog.Logger
	applicationCtxStopOnce   *sync.Once
}

func (app *ApplicationSingleton) Stop() {
	app.applicationCtxStopOnce.Do(func() {
		app.applicationContextCancel()
	})
}

func (app *ApplicationSingleton) GetApplicationCtx() context.Context {
	return app.applicationCtx
}

func (app *ApplicationSingleton) GetWorkerId() string {
	return app.workerId
}

func (app *ApplicationSingleton) GetLogger() *slog.Logger {
	return app.logger
}

func NewApplicationSingletonWithContext(ctx context.Context) *ApplicationSingleton {
	ctx, cancel := context.WithCancel(ctx)
	return &ApplicationSingleton{
		applicationCtx:           ctx,
		applicationContextCancel: cancel,
		workerId:                 uuid.NewString(),
		logger:                   slog.With("gox-helix"),
		applicationCtxStopOnce:   &sync.Once{},
	}
}
