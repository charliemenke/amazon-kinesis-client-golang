package actions

import (
	"encoding/json"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type InitAction struct {
	RecordProcessor	kcl.RecordProcessor
	Input struct {
		ActionType    string `json:"action"`
		ShardId   string `json:"shardId"`
		SeqNum    string `json:"sequenceNumber"`
		SubSeqNum int `json:"subSequenceNumber"`
	}
}

func NewInitAction(rp kcl.RecordProcessor, input []byte) (*InitAction, error) {
	a := InitAction{RecordProcessor: rp}
	err := json.Unmarshal(input, &a.Input)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (a *InitAction) ActionType() string {
	return a.Input.ActionType
}

func (a *InitAction) Dispatch() error {
	err := a.RecordProcessor.Initialize(a.Input.ShardId, a.Input.SeqNum, a.Input.SubSeqNum)
	if err != nil {
		return err
	}
	return nil
}
