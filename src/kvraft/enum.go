package kvraft

// Errors
const (
	ERR_OK             = "" // no error
	ERR_NotLeader      = "Not leader"
	ERR_FailedToCommit = "Failed to commit"
	ERR_CommitTimeout  = "Commit timeout"
)

// Op type
const (
	OT_GET    = "Get"
	OT_PUT    = "Put"
	OT_APPEND = "Append"
)

// Op status
const (
	OPS_COMMITING = iota
	OPS_OK
	OPS_FAILED
)
