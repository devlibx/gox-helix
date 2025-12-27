package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"sync"
)

type Factory interface {
	GetOrCreateDomainTasklistProcessor(ctx context.Context, domain string) (DomainTasklistProcessor, error)
}

type factoryImpl struct {
	gox.CrossFunction
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
			f.lockService,
			domain,
		)
	}
	return f.DomainTasklistProcessors[domain], nil
}

func NewProcessorFactory(
	cf gox.CrossFunction,
	lockService locker.Locker,
) Factory {
	f := &factoryImpl{
		CrossFunction:                 cf,
		lockService:                   lockService,
		DomainTasklistProcessors:      map[string]DomainTasklistProcessor{},
		DomainTasklistProcessorsMutex: &sync.Mutex{},
	}
	return f
}
