package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
)

type tasklistProcessorImpl struct {
	gox.CrossFunction
	domain    string
	tasklist  string
	partition int
}

func (t tasklistProcessorImpl) Start(ctx context.Context) (*TasklistProcessResponse, error) {
	return nil, nil
}

func (t tasklistProcessorImpl) Stop(ctx context.Context) error {
	return nil
}

func NewTasklistProcessor(
	cf gox.CrossFunction,
	domain string,
	tasklist string,
	partition int,
) TasklistProcessor {
	p := &tasklistProcessorImpl{
		CrossFunction: cf,
		domain:        domain,
		tasklist:      tasklist,
		partition:     partition,
	}
	return p
}
