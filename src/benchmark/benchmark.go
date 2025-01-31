package benchmark

type Benchmark interface {
	Init()
	Prepare()
	Event()
	Done()
	Cleanup()
}
