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
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"strconv"
	"sync"
)

type Persister struct {
	mu     sync.Mutex
	mid    int // machine ID
	rsfp   *os.File
	snapfp *os.File
}

func MakePersister() *Persister {
	ps := Persister{}
	ps.mid = rand.Int()

	ps.rsfp, _ = os.OpenFile("raftstate/raftstate-"+strconv.Itoa(ps.mid)+".dat", os.O_CREATE|os.O_RDWR, 0666)
	ps.snapfp, _ = os.OpenFile("snapshot/snapshot-"+strconv.Itoa(ps.mid)+".dat", os.O_CREATE|os.O_RDWR, 0666)

	return &ps
}

func MakePersisterWithId(id int) *Persister {
	ps := Persister{}
	ps.mid = id

	ps.rsfp, _ = os.OpenFile("raftstate/raftstate-"+strconv.Itoa(ps.mid)+".dat", os.O_CREATE|os.O_RDWR, 0666)
	ps.snapfp, _ = os.OpenFile("snapshot/snapshot-"+strconv.Itoa(ps.mid)+".dat", os.O_CREATE|os.O_RDWR, 0666)

	return &ps
}

// func clone(orig []byte) []byte {
// 	x := make([]byte, len(orig))
// 	copy(x, orig)
// 	return x
// }

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersisterWithId(ps.mid)
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.rsfp.Seek(0, 0)

	var raftStateSize int32
	err := binary.Read(ps.rsfp, binary.LittleEndian, &raftStateSize)
	if err != nil && err.Error() == "EOF" { // fix for startup
		return nil
	} else if err != nil {
		panic(err)
	}

	raftState := make([]byte, raftStateSize)
	err = binary.Read(ps.rsfp, binary.LittleEndian, &raftState)
	if err != nil {
		panic(err)
	}

	return raftState
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.rsfp.Seek(0, 0)

	var raftStateSize int32
	err := binary.Read(ps.rsfp, binary.LittleEndian, &raftStateSize)
	if err != nil && err.Error() == "EOF" { // fix for startup
		return 0
	} else if err != nil {
		panic(err)
	}
	return int(raftStateSize)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.rsfp.Seek(0, 0)
	ps.snapfp.Seek(0, 0)

	raftstateBuf := new(bytes.Buffer)
	if err := binary.Write(raftstateBuf, binary.LittleEndian, int32(len(raftstate))); err != nil {
		panic(err)
	}
	if err := binary.Write(raftstateBuf, binary.LittleEndian, raftstate); err != nil {
		panic(err)
	}
	binary.Write(ps.rsfp, binary.LittleEndian, raftstateBuf.Bytes())

	snapshotBuf := new(bytes.Buffer)
	if err := binary.Write(snapshotBuf, binary.LittleEndian, int32(len(snapshot))); err != nil {
		panic(err)
	}
	if err := binary.Write(snapshotBuf, binary.LittleEndian, snapshot); err != nil {
		panic(err)
	}
	binary.Write(ps.snapfp, binary.LittleEndian, snapshotBuf.Bytes())

	ps.rsfp.Sync()
	ps.snapfp.Sync()
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.snapfp.Seek(0, 0)

	var snapshotSize int32
	err := binary.Read(ps.snapfp, binary.LittleEndian, &snapshotSize)
	if err != nil && err.Error() == "EOF" {
		return nil
	} else if err != nil {
		panic(err)
	}

	snapshot := make([]byte, snapshotSize)
	err = binary.Read(ps.snapfp, binary.LittleEndian, &snapshot)
	if err != nil {
		panic(err)
	}
	return snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.snapfp.Seek(0, 0)

	var snapshotSize int32
	err := binary.Read(ps.snapfp, binary.LittleEndian, &snapshotSize)
	if err != nil && err.Error() == "EOF" {
		return 0
	} else if err != nil {
		panic(err)
	}
	return int(snapshotSize)
}
