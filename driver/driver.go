package driver

type (
	Driver interface {
		Open(string)
		ExecuteTransaction([]string)
		Close()
	}
)
