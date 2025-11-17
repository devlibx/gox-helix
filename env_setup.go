package helix

import (
	"bufio"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

//go:embed env/common.env
var commonEnv string

//go:embed env/dev.env
var devEnv string

func SetupCommonEnv() {
	scanner := bufio.NewScanner(strings.NewReader(commonEnv))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			_ = os.Setenv(parts[0], parts[1])
			slog.Debug("Setting env variable from common env file", parts[0], parts[1])
		}
	}
}

func SetupTestEnv() {
	SetupCommonEnv()
	scanner := bufio.NewScanner(strings.NewReader(devEnv))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			_ = os.Setenv(parts[0], parts[1])
			slog.Debug("Setting env variable from dev env file", parts[0], parts[1])
		}
	}
}

func GetDefaultSqlUrl() string {
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbName)
	return url
}
