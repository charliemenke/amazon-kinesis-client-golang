package kcl

import (
	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
)

type Record struct {
	Action                      string `json:"action"`
	Data                        string `json:"data"`
	PartitionKey                string `json:"partitionKey"`
	ApproximateArrivalTimestamp int `json:"approximateArrivalTimestamp"`
	SequenceNumber              string `json:"sequenceNumber"`
	SubSequenceNumber           int `json:"subSequenceNumber"`
}

type RecordProcessor interface {
	Initialize(shardId, seqNum string, subSeqNum int) error
	ProcessRecords(records []Record, lag int, cp *checkpoint.Checkpointer) error
	LeaseLost() error
	ShardEnded(shardSeqEnd string, cp *checkpoint.Checkpointer) error
	ShutdownRequested(cp *checkpoint.Checkpointer) error
}
