package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
)

type tasklistProcessorImpl struct {
	gox.CrossFunction
	stopSignal  *common.ApplicationStopSignal
	lockService locker.Locker
	domain      string
	tasklist    string
	partition   int
}

func (t tasklistProcessorImpl) Start(ctx context.Context) (*TasklistProcessResponse, error) {
	return nil, nil
}

func (t tasklistProcessorImpl) Stop(ctx context.Context) error {
	return nil
}

func NewTasklistProcessor(
	cf gox.CrossFunction,
	stopSignal *common.ApplicationStopSignal,
	lockService locker.Locker,
	domain string,
	tasklist string,
	partition int,
) TasklistProcessor {
	p := &tasklistProcessorImpl{
		CrossFunction: cf,
		stopSignal:    stopSignal,
		lockService:   lockService,
		domain:        domain,
		tasklist:      tasklist,
		partition:     partition,
	}
	return p
}
