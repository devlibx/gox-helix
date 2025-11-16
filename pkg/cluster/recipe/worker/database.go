package worker

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type DataLayer struct {
	gox.CrossFunction
	helixWorkerMysql.Querier
}

func NewWorkerDataLayer(cf gox.CrossFunction, ch databaseCommon.ConnectionHolder) (*DataLayer, error) {
	q, err := helixWorkerMysql.Prepare(context.Background(), ch.GetHelixMasterDbConnection())
	if err != nil {
		return nil, errors.Wrap(err, "error in connecting to database  - failed to call prepare helix worker database")
	}
	return &DataLayer{
		CrossFunction: cf,
		Querier:       q,
	}, err
}
