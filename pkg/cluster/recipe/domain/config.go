package domain

type Config struct {
	Domain  string
	Domains []TaskList
}

type TaskList struct {
	Name           string
	PartitionCount int
}
