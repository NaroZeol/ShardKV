package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"os"
	"sync"
)

type Persister struct {
	mu     sync.Mutex
	gid    int
	id     int
	rsfp   *os.File
	snapfp *os.File
}

func MakePersister(gid int, id int) *Persister {
	ps := Persister{}
	ps.gid = gid
	ps.id = id

	var err1, err2 error

	err1 = os.MkdirAll("raftstate", 0777)
	err2 = os.MkdirAll("snapshot", 0777)

	if err1 != nil || err2 != nil {
		panic("Error creating directories")
	}

	ps.rsfp, err1 = os.OpenFile(fmt.Sprintf("raftstate/raftstate-%d-%d.dat", ps.gid, ps.id), os.O_CREATE|os.O_RDWR, 0666)
	ps.snapfp, err2 = os.OpenFile(fmt.Sprintf("snapshot/snapshot-%d-%d.dat", ps.gid, ps.id), os.O_CREATE|os.O_RDWR, 0666)

	if err1 != nil || err2 != nil {
		panic("Error opening files")
	}

	return &ps
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.rsfp.Seek(0, 0)

	finfo, err := ps.rsfp.Stat()
	if err != nil {
		panic(err)
	}

	raftState := make([]byte, finfo.Size())
	size, err := ps.rsfp.Read(raftState)
	if err != nil || size != len(raftState) {
		panic("Error reading from file")
	}

	return raftState
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	finfo, err := ps.rsfp.Stat()
	if err != nil {
		panic(err)
	}

	return int(finfo.Size())
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.rsfp.Seek(0, 0)
	ps.snapfp.Seek(0, 0)

	size1, err1 := ps.rsfp.Write(raftstate)
	size2, err2 := ps.snapfp.Write(snapshot)

	if err1 != nil || err2 != nil || size1 != len(raftstate) || size2 != len(snapshot) {
		panic("Error writing to files")
	}

	ps.rsfp.Truncate(int64(len(raftstate)))
	ps.snapfp.Truncate(int64(len(snapshot)))

	ps.rsfp.Sync()
	ps.snapfp.Sync()
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.snapfp.Seek(0, 0)

	finfo, err := ps.snapfp.Stat()
	if err != nil {
		panic(err)
	}

	snapshot := make([]byte, finfo.Size())
	size, err := ps.snapfp.Read(snapshot)
	if err != nil || size != len(snapshot) {
		panic("Error reading from file")
	}

	return snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	finfo, err := ps.snapfp.Stat()
	if err != nil {
		panic(err)
	}

	return int(finfo.Size())
}
