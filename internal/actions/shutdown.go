package actions

import (
	"encoding/json"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type ShutdownAction struct {
	RecordProcessor	kcl.RecordProcessor
	Checkpointer *checkpoint.Checkpointer
	Input struct {
		ActionType             string       `json:"action"`
		Reason	string	`json:"reason"`
	}
}

func NewShutdownAction(rp kcl.RecordProcessor, cp *checkpoint.Checkpointer, input []byte) (*ShutdownAction, error) {
	a := ShutdownAction{RecordProcessor: rp, Checkpointer: cp}
	err := json.Unmarshal(input, &a.Input)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (a *ShutdownAction) ActionType() string {
	return a.Input.ActionType
}

func (a *ShutdownAction) Dispatch() error {
	err := a.RecordProcessor.Shutdown(a.Input.Reason, a.Checkpointer)
	if err != nil {
		return err
	}
	return nil
}
