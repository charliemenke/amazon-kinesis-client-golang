package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
	kclmanager "github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/kcl_manager"
)

type SimpleRecordProcessor struct {
	ShardId                string
	SeqNum                 string
	SubSeqNum              int
	LastProcessedSeqNum    string
	LastProcessedSubSeqNum int
}

func (rp *SimpleRecordProcessor) Initialize(shardId, seqNum string, subSeqNum int) error {
	rp.ShardId = shardId
	rp.SeqNum = seqNum
	rp.SubSeqNum = subSeqNum
	slog.Info("SimpleRecordProcessor starting....")
	slog.Info("SimpleRecordProcess initialized", "shard_id", rp.ShardId, "start_seq_num", rp.SeqNum, "start_sub_seq_num", rp.SubSeqNum)
	return nil
}

func (rp *SimpleRecordProcessor) ProcessRecords(records []kcl.Record, lag int, cp *checkpoint.Checkpointer) error {
	slog.Info("got records to process", "amount", len(records), "lag_in_ms", lag)
	for _, r := range records {
		slog.Info(fmt.Sprintf("record [ partKey: %s, seqNum: %s, subSeqNum: %d, arrivalTS: %d ]\n", r.PartitionKey, r.SequenceNumber, r.SubSequenceNumber, r.ApproximateArrivalTimestamp))
		slog.Info(r.Data)
		rp.LastProcessedSeqNum = r.SequenceNumber
		rp.LastProcessedSubSeqNum = r.SubSequenceNumber
	}
	err := cp.CheckpointBatch()
	if err != nil {
		return err
	}
	return nil
}

func (rp *SimpleRecordProcessor) LeaseLost() error {
	slog.Info("SimpleRecordProcessor lost its lease")
	return nil
}

func (rp *SimpleRecordProcessor) ShardEnded(shardSeqNum string, cp *checkpoint.Checkpointer) error {
	slog.Info("kinesis shard fully consumed", "shard_id", rp.ShardId, "shard_end_seq_num", shardSeqNum)
	err := cp.CheckpointSeqNum(shardSeqNum)
	if err != nil {
		return err
	}
	return nil
}

func (rp *SimpleRecordProcessor) Shutdown(reason string, cp *checkpoint.Checkpointer) error {
	if reason == "TERMINATE" || reason == "REQUESTED" {
		err := cp.CheckpointSeqNum(rp.LastProcessedSeqNum)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rp *SimpleRecordProcessor) ShutdownRequested(cp *checkpoint.Checkpointer) error {
	err := cp.CheckpointSeqNum(rp.LastProcessedSeqNum)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	kcl := kclmanager.NewKCLManager(os.Stdin, os.Stdout, os.Stderr, &SimpleRecordProcessor{})
	kcl.Run()
}
