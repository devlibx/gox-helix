package worker

import "context"

func (wl *DataLayer) GetActiveWorkers(ctx context.Context, domain string) ([]string, error) {
	return wl.GetAllActiveWorkersByDomain(ctx, domain)
}
