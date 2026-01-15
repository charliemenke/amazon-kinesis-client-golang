package kcl

import (
	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
)

// Record defines the json shape of a Kinesis "record" as
// it comes over in the KCL Multilang process action 
// ProcessRecords request.
type Record struct {
	Action                      string `json:"action"`
	Data                        string `json:"data"`
	PartitionKey                string `json:"partitionKey"`
	ApproximateArrivalTimestamp int `json:"approximateArrivalTimestamp"`
	SequenceNumber              string `json:"sequenceNumber"`
	SubSequenceNumber           int `json:"subSequenceNumber"`
}

// RecordProcessor is a quick start interface that you can implement
// and use with KCLManager to quickly start consuming records from a 
// kinesis stream
type RecordProcessor interface {
	Initialize(shardId, seqNum string, subSeqNum int) error
	ProcessRecords(records []Record, lag int, cp *checkpoint.Checkpointer) error
	LeaseLost() error
	ShardEnded(cp *checkpoint.Checkpointer) error
	ShutdownRequested(cp *checkpoint.Checkpointer) error
}
