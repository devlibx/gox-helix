package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type DataLayer struct {
	gox.CrossFunction
	helixClusterMysql.Querier
	queries *helixClusterMysql.Queries
}

func NewCoordinatorDataLayer(cf gox.CrossFunction, ch databaseCommon.ConnectionHolder) (*DataLayer, error) {
	q, err := helixClusterMysql.Prepare(context.Background(), ch.GetHelixMasterDbConnection())
	if err != nil {
		return nil, errors.Wrap(err, "error in connecting to database  - failed to call prepare helix coordinator database")
	}
	return &DataLayer{
		CrossFunction: cf,
		Querier:       q,
		queries:       q,
	}, err
}
