package domain

import "context"

type Service interface {
	Init(ctx context.Context) error
}
