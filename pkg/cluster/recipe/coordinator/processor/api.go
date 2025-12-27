package processor

import (
	"context"
	"fmt"
)

//go:generate mockgen -source=api.go -destination=mocks.go -package=processor

type DomainTasklistProcessRequest struct {
	Tasklist   string `json:"tasklist"`
	Partitions []int  `json:"partitions"`
}

func (d *DomainTasklistProcessRequest) String() string {
	if len(d.Partitions) > 0 {
		return fmt.Sprintf("DomainTasklistProcessRequest: allocated_partitions=%v", d.Partitions)
	}
	return fmt.Sprintf("DomainTasklistProcessRequest: partitions=[]")
}

type DomainTasklistProcessResponse struct {
}

type DomainTasklistProcessor interface {
	Process(ctx context.Context, request DomainTasklistProcessRequest) (*DomainTasklistProcessResponse, error)
}

type TasklistProcessResponse struct {
}

type TasklistProcessor interface {
	Start(ctx context.Context) (*TasklistProcessResponse, error)
	Stop(context.Context) error
}
