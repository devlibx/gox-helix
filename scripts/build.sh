cd pkg/cluster/recipe/coordinator/database && sqlc generate
cd -
cd pkg/cluster/recipe/domain/database & sqlc generate
cd -
cd pkg/cluster/recipe/lock/database & sqlc generate
cd -
cd pkg/cluster/recipe/worker/database & sqlc generate
cd -