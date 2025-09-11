package database

import (
	"database/sql"
)

type ConnectionHolder interface {
	GetHelixMasterDbConnection() *sql.DB
}

type connectionHolderImpl struct {
	HelixDbMasterConnection *sql.DB
}

func (c *connectionHolderImpl) GetHelixMasterDbConnection() *sql.DB {
	return c.HelixDbMasterConnection
}

func NewConnectionHolder(helixDbMasterConnection *sql.DB) ConnectionHolder {
	return &connectionHolderImpl{
		HelixDbMasterConnection: helixDbMasterConnection,
	}
}
