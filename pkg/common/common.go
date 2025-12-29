package common

import (
	"context"
	"github.com/google/uuid"
)

type ApplicationSingleton struct {
	applicationCtx           context.Context
	applicationContextCancel context.CancelFunc
	workerId                 string
}

func (app *ApplicationSingleton) Stop() {
	if app.applicationContextCancel != nil {
		app.applicationContextCancel()
	}
}

func (app *ApplicationSingleton) GetApplicationCtx() context.Context {
	return app.applicationCtx
}

func (app *ApplicationSingleton) GetWorkerId() string {
	return app.workerId
}

func GetDefaultApplicationSingleton() *ApplicationSingleton {
	ctx, cancel := context.WithCancel(context.Background())
	return &ApplicationSingleton{
		applicationCtx:           ctx,
		applicationContextCancel: cancel,
		workerId:                 uuid.NewString(),
	}
}

func GetDefaultApplicationSingletonWithContext(ctx context.Context) *ApplicationSingleton {
	ctx, cancel := context.WithCancel(ctx)
	return &ApplicationSingleton{
		applicationCtx:           ctx,
		applicationContextCancel: cancel,
		workerId:                 uuid.NewString(),
	}
}
