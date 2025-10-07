package locker

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixLockMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type DataLayer struct {
	gox.CrossFunction
	helixLockMysql.Querier
}

func NewLockerDataLayer(cf gox.CrossFunction, ch databaseCommon.ConnectionHolder) (*DataLayer, error) {
	q, err := helixLockMysql.Prepare(context.Background(), ch.GetHelixMasterDbConnection())
	if err != nil {
		return nil, errors.Wrap(err, "error in connecting to database  - failed to call prepare helix locker database")
	}
	return &DataLayer{
		CrossFunction: cf,
		Querier:       q,
	}, err
}
