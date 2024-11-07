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
	OT_GET = iota
	OT_PUT
	OT_APPEND
)
