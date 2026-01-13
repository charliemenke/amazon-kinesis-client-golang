package actions

import (
	"encoding/json"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type LeaseLostAction struct {
	RecordProcessor	kcl.RecordProcessor
	Input struct {
		ActionType             string       `json:"action"`
	}
}

func NewLeaseLostAction(rp kcl.RecordProcessor, input []byte) (*LeaseLostAction, error) {
	a := LeaseLostAction{RecordProcessor: rp}
	err := json.Unmarshal(input, &a.Input)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (a *LeaseLostAction) ActionType() string {
	return a.Input.ActionType
}

func (a *LeaseLostAction) Dispatch() error {
	err := a.RecordProcessor.LeaseLost()
	if err != nil {
		return err
	}
	return nil
}
