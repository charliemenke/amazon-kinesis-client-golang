package kcl

import (
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/checkpoint"
)

// RecordProcessor is a quick start interface that you can implement
// and use with KCLManager to quickly start consuming records from a
// kinesis stream
type RecordProcessor interface {
	Initialize(shardId, seqNum string, subSeqNum int) error
	ProcessRecords(records []actions.Record, lag int, cp *checkpoint.Checkpointer) error
	LeaseLost() error
	ShardEnded(cp *checkpoint.Checkpointer) error
	ShutdownRequested(cp *checkpoint.Checkpointer) error
}
