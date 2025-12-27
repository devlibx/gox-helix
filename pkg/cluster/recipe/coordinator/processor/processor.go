package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"sync"
)

type Factory interface {
	GetOrCreateDomainTasklistProcessor(ctx context.Context, domain string) (DomainTasklistProcessor, error)

	Stop(ctx context.Context) error
}

type factoryImpl struct {
	gox.CrossFunction
	stopSignal                    *common.ApplicationStopSignal
	lockService                   locker.Locker
	DomainTasklistProcessors      map[string]DomainTasklistProcessor
	DomainTasklistProcessorsMutex *sync.Mutex
}

func (f *factoryImpl) GetOrCreateDomainTasklistProcessor(ctx context.Context, domain string) (DomainTasklistProcessor, error) {
	f.DomainTasklistProcessorsMutex.Lock()
	defer f.DomainTasklistProcessorsMutex.Unlock()

	if _, ok := f.DomainTasklistProcessors[domain]; !ok {
		f.DomainTasklistProcessors[domain] = NewDomainTasklistProcessor(
			f.CrossFunction,
			f.stopSignal,
			f.lockService,
			domain,
		)
	}
	return f.DomainTasklistProcessors[domain], nil
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
	stopSignal *common.ApplicationStopSignal,
	lockService locker.Locker,
) Factory {
	f := &factoryImpl{
		CrossFunction:                 cf,
		stopSignal:                    stopSignal,
		lockService:                   lockService,
		DomainTasklistProcessors:      map[string]DomainTasklistProcessor{},
		DomainTasklistProcessorsMutex: &sync.Mutex{},
	}

	// Make sure we stop everything when we get stop signal
	go func() {
		<-stopSignal.Ctx.Done()
		_ = f.Stop(context.Background())
	}()

	return f
}
