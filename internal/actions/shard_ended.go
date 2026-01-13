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
		Checkpoint             string       `json:"checkpoint"`
	}
}

func NewShardEndedAction(rp kcl.RecordProcessor, input []byte) (*ShardEndedAction, error) {
	a := ShardEndedAction{RecordProcessor: rp}
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
	err := a.RecordProcessor.ShardEnded(a.Input.Checkpoint, a.Checkpointer)
	if err != nil {
		return err
	}
	return nil
}
