package coordinator

import "context"

type ClientFunctionProcessWorkFunc func(ctx context.Context, work Work)

type ClientFunctionProcessWork func() ClientFunctionProcessWorkFunc
