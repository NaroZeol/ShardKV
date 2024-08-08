package mr

type workerState uint32

const (
	Free    workerState = 0
	Ready   workerState = 1
	Working workerState = 2
	Death   workerState = 3
)
