package worker

import (
	"context"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
)

func (wl *DataLayer) GetActiveWorkers(ctx context.Context, domain string) ([]string, error) {
	q := wl.Queries
	if queriesFromCtx, ok := ctx.Value("*helixWorkerMysql.Queries").(*helixWorkerMysql.Queries); ok {
		q = queriesFromCtx
	}
	return q.GetAllActiveWorkersByDomain(ctx, domain)
}
