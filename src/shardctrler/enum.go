package shardctrler

// Errors
const (
	ERR_OK             = "" // no error
	ERR_NotLeader      = "Not leader"
	ERR_FailedToCommit = "Failed to commit"
	ERR_CommitTimeout  = "Commit timeout"
)

// Op types
const (
	OT_Join  = "Join"
	OT_Leave = "Leave"
	OT_Move  = "Move"
	OT_Query = "Query"
)
