package coordinator

import "context"

type Coordinator interface {
	Start(ctx context.Context) error
}
