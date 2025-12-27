# This is a file which will do everything that is needed to be done before a commit
# This is a very extensive script to make sure everything is good
go install go.uber.org/mock/mockgen@latest
export INTEGRATION_TESTS=true
git pull
go mod tidy
find . -type f -name '*.go-e' -exec rm {} \;
find . -type f -name '*_mock.go' -exec rm {} \;
go generate ./...
go fmt ./...
go test -count=1 ./...