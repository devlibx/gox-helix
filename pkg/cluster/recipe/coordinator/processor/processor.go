package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"sync"
)

type Factory interface {
	GetOrCreateDomainTasklistProcessor(ctx context.Context, domain string) (DomainTasklistProcessor, error)
}

type factoryImpl struct {
	gox.CrossFunction
	DomainTasklistProcessors      map[string]DomainTasklistProcessor
	DomainTasklistProcessorsMutex *sync.Mutex
}

func (f *factoryImpl) GetOrCreateDomainTasklistProcessor(ctx context.Context, domain string) (DomainTasklistProcessor, error) {
	f.DomainTasklistProcessorsMutex.Lock()
	defer f.DomainTasklistProcessorsMutex.Unlock()

	if _, ok := f.DomainTasklistProcessors[domain]; !ok {
		f.DomainTasklistProcessors[domain] = NewDomainTasklistProcessor(f.CrossFunction, domain)
	}
	return f.DomainTasklistProcessors[domain], nil
}

func NewProcessorFactory(
	cf gox.CrossFunction,
) Factory {
	f := &factoryImpl{
		CrossFunction:                 cf,
		DomainTasklistProcessors:      map[string]DomainTasklistProcessor{},
		DomainTasklistProcessorsMutex: &sync.Mutex{},
	}
	return f
}
