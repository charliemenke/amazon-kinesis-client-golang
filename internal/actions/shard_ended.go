package actions

import (
	"encoding/json"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type ShardEndedAction struct {
	RecordProcessor	kcl.RecordProcessor
	Checkpointer *checkpoint.Checkpointer
	Input struct {
		ActionType             string       `json:"action"`
	}
}

func NewShardEndedAction(rp kcl.RecordProcessor, cp *checkpoint.Checkpointer, input []byte) (*ShardEndedAction, error) {
	a := ShardEndedAction{RecordProcessor: rp, Checkpointer: cp}
	err := json.Unmarshal(input, &a.Input)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (a *ShardEndedAction) ActionType() string {
	return a.Input.ActionType
}

func (a *ShardEndedAction) Dispatch() error {
	err := a.RecordProcessor.ShardEnded(a.Checkpointer)
	if err != nil {
		return err
	}
	return nil
}
