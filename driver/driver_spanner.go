package driver

type (
	DriverSpanner struct {
	}
)

func (s *DriverSpanner) Open(dsn string) {
}

func (s *DriverSpanner) ExecuteTransaction(stmts []string) {
}

func (s *DriverSpanner) Close() {
}

func (s *DriverSpanner) DSN(p string) {
}
