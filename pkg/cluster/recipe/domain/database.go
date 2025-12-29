package domain

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type DataLayer struct {
	gox.CrossFunction
	helixDomainMysql.Querier
	*helixDomainMysql.Queries
}

func NewDomainDataLayer(cf gox.CrossFunction, ch databaseCommon.ConnectionHolder) (*DataLayer, error) {
	q, err := helixDomainMysql.Prepare(context.Background(), ch.GetHelixMasterDbConnection())
	if err != nil {
		return nil, errors.Wrap(err, "error in connecting to database  - failed to call prepare helix coordinator database")
	}
	return &DataLayer{
		CrossFunction: cf,
		Querier:       q,
		Queries:       q,
	}, err
}

func (d *DataLayer) GetTaskListInfo(ctx context.Context, domain string, tasklist string) (*helixDomainMysql.HelixDomain, error) {
	return d.Querier.GetDomainByDomainAndTasklist(ctx, helixDomainMysql.GetDomainByDomainAndTasklistParams{
		Domain:   domain,
		Tasklist: tasklist,
	})
}
