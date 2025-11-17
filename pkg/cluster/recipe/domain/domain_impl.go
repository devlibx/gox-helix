package domain

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	"github.com/google/uuid"
)

type domainImpl struct {
	gox.CrossFunction
	dataLayer *DataLayer
	config    Config
	nodeId    string
}

func NewService(cf gox.CrossFunction, dataLayer *DataLayer, config Config) (Service, error) {
	return &domainImpl{
		CrossFunction: cf,
		dataLayer:     dataLayer,
		config:        config,
		nodeId:        uuid.NewString(),
	}, nil
}

func (s *domainImpl) Init(ctx context.Context) error {
	for _, taskList := range s.config.Domains {
		err := s.dataLayer.UpsertTasklist(ctx, helixDomainMysql.UpsertTasklistParams{
			Domain:         s.config.Domain,
			Tasklist:       taskList.Name,
			Metadata:       sql.NullString{Valid: true, String: `{}`},
			PartitionCount: uint32(taskList.PartitionCount),
		})
		if err != nil {
			return errors.Wrap(err, "failed to upsert domain=%s, taskList=%s", s.config.Domain, taskList.Name)
		}
	}
	return nil
}
