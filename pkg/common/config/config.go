package config

type Config struct {
	Domains map[string]*Domain `yaml:"domains" json:"domains"`
}

type Domain struct {
	Name                       string               `yaml:"name" json:"name"`
	WorkerCountToProcessDomain int                  `yaml:"worker_count_to_process_domain", json:"worker_count_to_process_domain"`
	Disabled                   bool                 `yaml:"disabled" json:"disabled"`
	TaskLists                  map[string]*TaskList `yaml:"task_list" json:"task_list"`
}

type TaskList struct {
	Disabled       bool   `yaml:"disabled" json:"disabled"`
	Name           string `yaml:"name" json:"name"`
	PartitionCount int    `yaml:"partition_count" json:"partition_count"`
}

func (c *Config) SetDefaults() {
	if c.Domains == nil {
		c.Domains = map[string]*Domain{}
		return
	}

	for name, d := range c.Domains {
		d.Name = name
		if d.Disabled {
			continue
		}

		if d.WorkerCountToProcessDomain <= 0 {
			d.WorkerCountToProcessDomain = 1
		}

		if d.TaskLists == nil {
			d.TaskLists = map[string]*TaskList{}
		}

		for tName, t := range d.TaskLists {
			t.Name = tName
			if t.Disabled {
				continue
			}
			if t.PartitionCount <= 0 {
				t.PartitionCount = 1
			}
		}
	}
}
