package coordinator

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	goxSql "github.com/devlibx/gox-base/v2/database/sql"
	"github.com/devlibx/gox-base/v2/errors"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	"github.com/google/uuid"
	"log/slog"
)

type singletonImpl struct {
	gox.CrossFunction
	dataLayer DataLayer
	config    Config
	nodeId    string
}

func (s *singletonImpl) Init(ctx context.Context) error {
	err := s.dataLayer.UpsertTasklist(ctx, helixClusterMysql.UpsertTasklistParams{
		Domain:         s.config.Domain,
		Tasklist:       s.config.TaskList,
		Metadata:       sql.NullString{Valid: true, String: `{}`},
		PartitionCount: uint32(s.config.PartitionCount),
	})
	if err != nil {
		return errors.Wrap(err, "failed to upsert domain=%s, taskList=%s", s.config.Domain, s.config.TaskList)
	}
	return nil
}

func (s *singletonImpl) BecomeMaster(ctx context.Context) error {
	ignoreErr := s.dataLayer.InsertDomainWorker(ctx, helixClusterMysql.InsertDomainWorkerParams{
		Domain:     s.config.Domain,
		UniqueID:   s.nodeId,
		Metadata:   goxSql.StringToSqlNullString(""),
		LastHbTime: s.Now(),
	})
	if ignoreErr != nil {
		slog.Debug("(non-critical) failed to insert worker", slog.String("domain", s.config.Domain), slog.String("nodId", s.nodeId))
	}

	return nil
}

func NewTasklistSingleton(
	cf gox.CrossFunction,
	dataLayer DataLayer,
	config Config,
) (Singleton, error) {
	nodeId := uuid.NewString()
	s := &singletonImpl{
		CrossFunction: cf,
		dataLayer:     dataLayer,
		config:        config,
		nodeId:        nodeId,
	}
	return s, nil
}
