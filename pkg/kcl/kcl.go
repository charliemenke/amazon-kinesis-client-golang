package kcl

import "github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"

type Record struct {
	Action                      string `json:"action"`
	Data                        string `json:"data"`
	PartitionKey                string `json:"partitionKey"`
	ApproximateArrivalTimestamp string `json:"approximateArrivalTimestamp"`
	SequenceNumber              string `json:"sequenceNumber"`
	SubSequenceNumber           string `json:"subSequenceNumber"`
}

type RecordProcessor interface {
	Initialize(shardId, seqNum, subSeqNum string) error
	ProcessRecords([]Record, *checkpoint.Checkpointer) error
	Shutdown(reason string)
}

type SimpleRecordProcessor struct {
}

func (rp *SimpleRecordProcessor) Initialize() error {
	return nil
}

func (rp *SimpleRecordProcessor) ProcessRecords(records []string) {
	return
}

func (rp *SimpleRecordProcessor) Shutdown(reason string) {
	return
}
