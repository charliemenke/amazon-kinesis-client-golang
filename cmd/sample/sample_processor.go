package main

import (
	"encoding/base64"
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
	Loggr                  *slog.Logger
}

func (rp *SimpleRecordProcessor) Initialize(shardId, seqNum string, subSeqNum int) error {
	rp.ShardId = shardId
	rp.SeqNum = seqNum
	rp.SubSeqNum = subSeqNum
	rp.Loggr.Info("SimpleRecordProcessor starting....")
	rp.Loggr.Info("SimpleRecordProcess initialized", "shard_id", rp.ShardId, "start_seq_num", rp.SeqNum, "start_sub_seq_num", rp.SubSeqNum)
	return nil
}

func (rp *SimpleRecordProcessor) ProcessRecords(records []kcl.Record, lag int, cp *checkpoint.Checkpointer) error {
	rp.Loggr.Info("got records to process", "amount", len(records), "lag_in_ms", lag)
	for _, r := range records {
		rp.Loggr.Info(fmt.Sprintf("record [ partKey: %s, seqNum: %s, subSeqNum: %d, arrivalTS: %d ]", r.PartitionKey, r.SequenceNumber, r.SubSequenceNumber, r.ApproximateArrivalTimestamp))
		kData, err := base64.StdEncoding.DecodeString(r.Data)
		if err != nil {
			return err
		}
		rp.Loggr.Info(string(kData))
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
	rp.Loggr.Info("SimpleRecordProcessor lost its lease")
	return nil
}

func (rp *SimpleRecordProcessor) ShardEnded(cp *checkpoint.Checkpointer) error {
	rp.Loggr.Info("kinesis shard fully consumed", "shard_id", rp.ShardId)
	err := cp.CheckpointBatch()
	if err != nil {
		return err
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
	loggr := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	kcl := kclmanager.NewKCLManager(
		os.Stdin,
		os.Stdout,
		&SimpleRecordProcessor{Loggr: loggr},
		kclmanager.WithLogger(loggr),
	)

	kcl.Run()
}
