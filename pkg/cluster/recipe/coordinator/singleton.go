package coordinator

import "context"

type Singleton interface {
	Init(ctx context.Context) error
	BecomeMaster(ctx context.Context) error
}
