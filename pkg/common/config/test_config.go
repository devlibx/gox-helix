package config

import (
	_ "embed"
	"github.com/devlibx/gox-base/v2/serialization"
)

//go:embed test_config.yaml
var testConfigStr string

func GetTestConfig() (*Config, error) {
	appConfig := Config{}
	err := serialization.ReadParameterizedYaml(testConfigStr, &appConfig, "env")
	if err != nil {
		return nil, err
	}
	appConfig.SetDefaults()
	return &appConfig, nil
}
