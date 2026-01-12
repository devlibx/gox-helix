package coordinator

import "context"

type ProcessWorkFuncInfo struct {
	Domain    string
	Tasklist  string
	Partition int
}

type ClientFunctionProcessWorkFunc func(ctx context.Context, work Work)

type ClientFunctionProcessWork func(info ProcessWorkFuncInfo) ClientFunctionProcessWorkFunc
