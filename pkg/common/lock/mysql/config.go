package helixLock

type MySqlConfig struct {
	Database             string `yaml:"database"`
	Host                 string `yaml:"host"`
	Port                 int    `yaml:"port"`
	User                 string `yaml:"user"`
	Password             string `yaml:"password"`
	MaxIdleConnection    int    `yaml:"max_idle_connections"`
	MaxOpenConnection    int    `yaml:"max_open_connections"`
	ConnMaxLifetimeInSec int    `yaml:"connection_max_lifetime_sec"`
	ConnMaxIdleTimeInSec int    `yaml:"connection_max_idle_time_sec"`
}

func (m *MySqlConfig) SetupDefault() {
	if m.Host == "" {
		m.Host = "localhost"
	}
	if m.Port <= 0 {
		m.Port = 3306
	}
	if m.MaxIdleConnection <= 0 {
		m.MaxIdleConnection = 10
	}
	if m.MaxOpenConnection <= 0 {
		m.MaxOpenConnection = 10
	}
	if m.ConnMaxLifetimeInSec <= 0 {
		m.ConnMaxLifetimeInSec = 60
	}
	if m.ConnMaxIdleTimeInSec <= 0 {
		m.ConnMaxIdleTimeInSec = 60
	}
}
