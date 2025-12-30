package processor

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"sync"
)

type CreateDomainTasklistProcessorRequest struct {
	Domain                    string
	TaskList                  string
	WorkerId                  string
	ClientFunctionProcessWork coordinator.ClientFunctionProcessWork
}

type Factory interface {
	GetOrCreateDomainTasklistProcessor(ctx context.Context, request CreateDomainTasklistProcessorRequest) (coordinator.DomainTasklistProcessor, error)

	Stop(ctx context.Context) error
}

type factoryImpl struct {
	gox.CrossFunction
	stopSignal                    *common.ApplicationSingleton
	lockService                   locker.Locker
	partitionService              coordinator.PartitionService
	DomainTasklistProcessors      map[string]coordinator.DomainTasklistProcessor
	DomainTasklistProcessorsMutex *sync.Mutex
	applicationSingleton          *common.ApplicationSingleton
}

func (f *factoryImpl) GetOrCreateDomainTasklistProcessor(ctx context.Context, request CreateDomainTasklistProcessorRequest) (coordinator.DomainTasklistProcessor, error) {
	f.DomainTasklistProcessorsMutex.Lock()
	defer f.DomainTasklistProcessorsMutex.Unlock()

	key := fmt.Sprintf("%s-%s-%s", request.Domain, request.TaskList, request.WorkerId)
	if _, ok := f.DomainTasklistProcessors[key]; !ok {
		f.DomainTasklistProcessors[key] = NewDomainTasklistProcessor(
			f.CrossFunction,
			f.lockService,
			f.partitionService,
			&DomainTasklistProcessorCfg{
				Domain:                    request.Domain,
				TaskList:                  request.TaskList,
				WorkerId:                  request.WorkerId,
				ClientFunctionProcessWork: request.ClientFunctionProcessWork,
			},
			f.applicationSingleton,
		)
	}
	return f.DomainTasklistProcessors[key], nil
}

func (f *factoryImpl) Stop(ctx context.Context) error {
	f.DomainTasklistProcessorsMutex.Lock()
	defer f.DomainTasklistProcessorsMutex.Unlock()
	for _, processor := range f.DomainTasklistProcessors {
		_ = processor.Stop(ctx)
	}
	return nil
}

func NewProcessorFactory(
	cf gox.CrossFunction,
	stopSignal *common.ApplicationSingleton,
	lockService locker.Locker,
	partitionService coordinator.PartitionService,
	applicationSingleton *common.ApplicationSingleton,
) Factory {
	f := &factoryImpl{
		CrossFunction:                 cf,
		stopSignal:                    stopSignal,
		lockService:                   lockService,
		partitionService:              partitionService,
		DomainTasklistProcessors:      map[string]coordinator.DomainTasklistProcessor{},
		DomainTasklistProcessorsMutex: &sync.Mutex{},
		applicationSingleton:          applicationSingleton,
	}

	// Make sure we stop everything when we get stop signal
	go func() {
		<-stopSignal.GetApplicationCtx().Done()
		_ = f.Stop(context.Background())
	}()

	return f
}
