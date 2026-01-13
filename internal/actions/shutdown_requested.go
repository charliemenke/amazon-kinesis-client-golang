package actions

import (
	"encoding/json"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type ShutdownRequestedAction struct {
	RecordProcessor	kcl.RecordProcessor
	Checkpointer *checkpoint.Checkpointer
	Input struct {
		ActionType             string       `json:"action"`
	}
}

func NewShutdownRequestedAction(rp kcl.RecordProcessor, cp *checkpoint.Checkpointer, input []byte) (*ShutdownRequestedAction, error) {
	a := ShutdownRequestedAction{RecordProcessor: rp, Checkpointer: cp}
	err := json.Unmarshal(input, &a.Input)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (a *ShutdownRequestedAction) ActionType() string {
	return a.Input.ActionType
}

func (a *ShutdownRequestedAction) Dispatch() error {
	err := a.RecordProcessor.ShutdownRequested(a.Checkpointer)
	if err != nil {
		return err
	}
	return nil
}
