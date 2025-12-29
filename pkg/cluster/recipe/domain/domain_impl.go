package domain

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	"github.com/devlibx/gox-helix/pkg/common/config"
	"log/slog"
)

type domainImpl struct {
	gox.CrossFunction
	dataLayer    *DataLayer
	domainConfig *config.Config
}

func NewService(cf gox.CrossFunction, dataLayer *DataLayer, domainConfig *config.Config) (Service, error) {
	return &domainImpl{
		CrossFunction: cf,
		dataLayer:     dataLayer,
		domainConfig:  domainConfig,
	}, nil
}

func (s *domainImpl) Start(ctx context.Context) error {
	for _, d := range s.domainConfig.Domains {
		for _, tl := range d.TaskLists {
			if tl.Disabled {
				slog.Warn("tasklist is disabled", "domain", d.Name, "tasklist", tl.Name)
				continue
			}
			if err := s.dataLayer.Querier.UpsertTasklist(ctx, helixDomainMysql.UpsertTasklistParams{
				Domain:         d.Name,
				Tasklist:       tl.Name,
				Metadata:       sql.NullString{Valid: true, String: `{}`},
				PartitionCount: uint32(tl.PartitionCount),
			}); err != nil {
				return errors.Wrap(err, "failed to upsert domain=%s, taskList=%s", d.Name, tl.Name)
			}
		}
	}
	return nil
}
