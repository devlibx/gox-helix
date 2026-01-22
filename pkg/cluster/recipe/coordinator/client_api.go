package coordinator

import (
	"context"
	"sync"
	"time"
)

type CreateWorkProcessFunctionInfo struct {
	Domain    string
	Tasklist  string
	Partition int
}

type ClientFunctionProcessor interface {
	Process(ctx context.Context, work Work)
	Shutdown(ctx context.Context)
	GetWaitTimeForWork(ctx context.Context, work Work) time.Duration
}

type ClientFunctionProvider interface {
	CreateWorkProcessFunction(ctx context.Context, info CreateWorkProcessFunctionInfo) ClientFunctionProcessor
}

type noOpClientFunctionProvider struct {
	stopOnce *sync.Once
}

func (r *noOpClientFunctionProvider) CreateWorkProcessFunction(ctx context.Context, info CreateWorkProcessFunctionInfo) ClientFunctionProcessor {
	return r
}

func (r *noOpClientFunctionProvider) Process(ctx context.Context, work Work) {
	work.CompletedChannel <- WorkResponse{}
	close(work.CompletedChannel)
}

func (r *noOpClientFunctionProvider) Shutdown(ctx context.Context) {
}

func (r *noOpClientFunctionProvider) GetWaitTimeForWork(ctx context.Context, work Work) time.Duration {
	return time.Duration(100 * time.Millisecond)
}

func NewNoOpClientFunctionProvider() ClientFunctionProvider {
	return &noOpClientFunctionProvider{stopOnce: &sync.Once{}}
}
