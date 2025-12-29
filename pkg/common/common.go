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
	loggers                  map[string]*slog.Logger
	loggersMutex             *sync.Mutex
}

func (app *ApplicationSingleton) GetModuleLogger(name string) *slog.Logger {
	logger := app.logger.With("module", name)
	app.loggersMutex.Lock()
	defer app.loggersMutex.Unlock()
	app.loggers[name] = logger
	return logger
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
	workerId := uuid.NewString()
	return &ApplicationSingleton{
		applicationCtx:           ctx,
		applicationContextCancel: cancel,
		workerId:                 workerId,
		logger:                   slog.With("app", "gox-helix").With("worker_id", workerId),
		applicationCtxStopOnce:   &sync.Once{},
		loggers:                  make(map[string]*slog.Logger),
		loggersMutex:             &sync.Mutex{},
	}
}
