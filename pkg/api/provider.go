package goxHelixApi

import (
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/processor"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/executor"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"go.uber.org/fx"
)

var Provider = fx.Options(
	processor.Provider,
	locker.Provider,
	coordinator.Provider,
	domain.Provider,
	worker.Provider,
	fx.Provide(executor.NewExecutor),
)
