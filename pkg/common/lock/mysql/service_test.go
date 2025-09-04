package helixLock

import (
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	"github.com/devlibx/gox-helix/pkg/util"
	"github.com/stretchr/testify/suite"
	"os"
	"strconv"
	"testing"
)

type ServiceTestSuite struct {
	suite.Suite
	service lock.Locker
}

func (s *ServiceTestSuite) SetupSuite() {
	// Load environment variables from .env file
	err := util.LoadDevEnv()
	s.Require().NoError(err, "Failed to load dev environment")

	// Create MySQL configuration from environment variables
	config := &MySqlConfig{
		Database: os.Getenv("MYSQL_DB"),
		Host:     os.Getenv("MYSQL_HOST"),
		User:     os.Getenv("MYSQL_USER"),
		Password: os.Getenv("MYSQL_PASSWORD"),
	}

	// Parse integer environment variables with defaults
	if port := os.Getenv("MYSQL_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Port = p
		}
	}

	if maxOpen := os.Getenv("MYSQL_MAX_OPEN_CONNECTIONS"); maxOpen != "" {
		if m, err := strconv.Atoi(maxOpen); err == nil {
			config.MaxOpenConnection = m
		}
	}

	if maxIdle := os.Getenv("MYSQL_MAX_IDLE_CONNECTIONS"); maxIdle != "" {
		if m, err := strconv.Atoi(maxIdle); err == nil {
			config.MaxIdleConnection = m
		}
	}

	if maxLifetime := os.Getenv("MYSQL_CONN_MAX_LIFETIME_SEC"); maxLifetime != "" {
		if m, err := strconv.Atoi(maxLifetime); err == nil {
			config.ConnMaxLifetimeInSec = m
		}
	}

	if maxIdleTime := os.Getenv("MYSQL_CONN_MAX_IDLE_TIME_SEC"); maxIdleTime != "" {
		if m, err := strconv.Atoi(maxIdleTime); err == nil {
			config.ConnMaxIdleTimeInSec = m
		}
	}

	// Create the service
	cf := gox.NewNoOpCrossFunction()
	service, err := NewHelixLockMySQLService(cf, config)
	s.Require().NoError(err, "Failed to create MySQL lock service")
	
	s.service = service
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(ServiceTestSuite))
}