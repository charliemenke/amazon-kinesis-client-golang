package kcl

import (
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/checkpoint"
)

// RecordProcessor is a quick start interface that you can implement
// and use with KCLManager to quickly start consuming records from a
// kinesis stream
type RecordProcessor interface {
	// Initialize should be called exactly once by the KCL Multilang
	// process. KCL will pass to your process the assisned shard's ID
	// and starting seq/sub seq num.
	Initialize(shardId, seqNum string, subSeqNum int) error
	// ProcessRecords is the method where you recording processing logic
	// should live. You can checkpoint at any point in this method using
	// the provided `Checkpointer` and its methods. Lag is the amount of/
	// milliseconds behind `records` is when compared to the lastest record
	// on the shard. It is a good way to monitor if your record processor is
	// able to keep up with production.
	ProcessRecords(records []actions.Record, lag int, cp *checkpoint.Checkpointer) error
	// LeaseLost is called by KCL to notifify your processor that it is no
	// longer assigned any shard and KCL will cease to communicate with your
	// processor. No checkpointer is available in this method as it is not
	// advised to checkpoint in this state because another worker (record
	// processor) may already be assined the lease.
	LeaseLost() error
	// ShardEnded is called by KCL to signel that the record processor has
	// reached the end of its assigned shard. You **must** call
	// `cp.CheckpointBatch()` at this time to allow KCL to clean up the
	// lease assignments and reshuffle work.
	ShardEnded(cp *checkpoint.Checkpointer) error
	// ShutdownRequested is called by KCL before a lease is lost or
	// application is shutdown to give your record processor a chance to
	// checkpoint its position.
	ShutdownRequested(cp *checkpoint.Checkpointer) error
}
