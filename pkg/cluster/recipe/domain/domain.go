package domain

import "context"

type Domain interface {
	Init(ctx context.Context) error
}
