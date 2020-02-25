package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

type DbConnection struct {
	Conn *sqlx.DB
}

func (p *DbConnection) Connect(dbType string, host, dbName, user, password string, port uint16) error {
	sqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require", host, port, user, password, dbName)
	conn, err := sqlx.Open(dbType, sqlInfo)

	if err != nil {
		return err
	}

	if err = conn.Ping(); err != nil {
		return err
	}

	p.Conn = conn

	return nil
}

func (p *DbConnection) Close() error {
	return p.Conn.Close()
}
