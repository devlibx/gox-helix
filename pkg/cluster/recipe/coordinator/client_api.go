package coordinator

import "context"

type ClientFunctionProcessWork func(ctx context.Context, work Work)
