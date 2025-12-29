package executor

import (
	"context"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/processor"
	"github.com/devlibx/gox-helix/pkg/common/config"
)

func (s *serviceImpl) startTasklistProcessorAndCallingClientWorkFunction(ctx context.Context, domain *config.Domain) error {
	for _, tl := range domain.TaskLists {
		_, _ = s.ProcessorFactory.GetOrCreateDomainTasklistProcessor(
			ctx,
			processor.CreateDomainTasklistProcessorRequest{
				Domain:                    domain.Name,
				TaskList:                  tl.Name,
				WorkerId:                  s.workerId,
				ClientFunctionProcessWork: s.ClientFunctionProcessWork,
			},
		)
	}
	return nil
}
