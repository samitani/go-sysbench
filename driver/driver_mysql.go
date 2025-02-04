package driver

type (
	DriverMySQL struct {
	}
)

func (s *DriverMySQL) Open(dsn string) {
}

func (s *DriverMySQL) ExecuteTransaction() {
}

func (s *DriverMySQL) Close() {
}

func (s *DriverMySQL) DSN(p string) {
}
