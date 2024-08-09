package mr

type workerState uint32

const (
	WS_Free    workerState = 0
	WS_Ready   workerState = 1
	WS_Working workerState = 2
	WS_Death   workerState = 3
	WS_Exiting workerState = 4
)
