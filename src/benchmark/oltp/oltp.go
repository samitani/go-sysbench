package oltp

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type Oltp struct{
	db *sql.DB
}

func (o *Oltp) Init() {
	return
}

func (o *Oltp) Prepare(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	o.db = db
	return nil
}

func (o *Oltp) Event() error {
	_, err := o.db.Exec("SELECT 1") // 任意のクエリを実行
	if err != nil {
		return err
	}
	return nil
}

func (o *Oltp) Done() {
	o.db.Close()
}

func (o *Oltp) Cleanup() {
	return
}
