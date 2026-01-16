package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/actions"
)

func main() {
	// create the low level kcl interfacer
	kclInterfacer := kcl.NewMultilangInterface(os.Stdin, os.Stdout)
	err := startUp(kclInterfacer)
	if err != nil {
		panic(err)
	}

	// read batches of 10 records from kinesis
	// only checkpoint after gathering at least
	// 10 records
	for {
		// read batch of kcl messages
		records, lastAction, err := handleKclBatch(kclInterfacer, 10)
		if err != nil {
			slog.Error("error reaching batch length", "error", err)
			os.Exit(1)
		}
		// handle records batch
		processRecords(records)

		// depending on the last action KCL gave us, we may or may not
		// be able to checkpoint
		switch lastAction {
		case actions.SHUTDOWN_REQUESTED, actions.PROCESS_RECORDS:
			kclInterfacer.Checkpointer.CheckpointSeqNum(records[len(records)-1].SequenceNumber)
		case actions.SHARD_ENDED:
			// need to call checkpointbatch in this case
			kclInterfacer.Checkpointer.CheckpointBatch()
		case actions.LEASE_LOST:
			// cannot checkpoint in this situation
			slog.Error("worker has lost least")
		default:
			slog.Error("error handling last action", "error", err)
			os.Exit(0)
		}
		kclInterfacer.WriteActionComplete(lastAction)
	}

}


func startUp(kcli *kcl.MultilangInterface) error {
	rAction, err := kcli.ReadActionRequest()
	if err != nil {
		return err
	}

	initAction, err := rAction.ToInitAction()
	if err != nil {
		return err
	}
	slog.Info("startting up kcl consumer", "shard_id", initAction.ShardId)
	err = kcli.WriteActionComplete(initAction.Action)
	if err != nil {
		return err
	}
	return nil
}

func handleKclBatch(kcli *kcl.MultilangInterface, batchLen int) ([]actions.Record, string, error) {
	recordsBatch := make([]actions.Record, 0, batchLen)

	for len(recordsBatch) < batchLen {
		slog.Info("waiting for records batch...", "batch_length", len(recordsBatch))
		rAction, err := kcli.ReadActionRequest()
		if err != nil {
			return recordsBatch, "", err
		}
		switch rAction.ActionType {
		case actions.SHUTDOWN_REQUESTED, actions.SHARD_ENDED, actions.LEASE_LOST:
			return recordsBatch, rAction.ActionType, nil
		case "processRecords":
			var a actions.ProcessAction
			a, err = rAction.ToProcessAction()
			if err != nil {
				return recordsBatch, a.Action, err
			}
			recordsBatch = append(recordsBatch, a.Records...)
			// if still below batch length, make sure to report that action is done for next loop
			if len(recordsBatch) <  batchLen { 
				kcli.WriteActionComplete(a.Action)
			}
		default:
			return recordsBatch, "", fmt.Errorf("unsupported action type: %s", rAction.ActionType)
		}
	}
	return recordsBatch, actions.PROCESS_RECORDS, nil
}

func processRecords(records []actions.Record) {
	for _, r := range records {
		slog.Info("record", "data", r.Data)
	}
}
