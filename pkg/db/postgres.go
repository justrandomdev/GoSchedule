package db

import (
	"fmt"

	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
)

type PostgresConnection struct {
	Conn *sqlx.DB
}

func (p *PostgresConnection) Connect(host, dbName, user, password string, port uint16) error {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require", host, port, user, password, dbName)
	conn, err := sqlx.Open("postgres", psqlInfo)

	if err != nil {
		return err
	}

	if err = conn.Ping(); err != nil {
		return err
	}

	p.Conn = conn

	return nil
}

func (p *PostgresConnection) Close() error {
	return p.Conn.Close()
}
