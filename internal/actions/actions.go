package actions

import (
	"encoding/json"
	"fmt"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type RawAction struct {
	ActionType string `json:"action"`
	Raw        []byte
}

func (a *RawAction) UnmarshalJSON(data []byte) error {
	type Alias RawAction
	var tmp Alias
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	*a = RawAction(tmp)
	a.Raw = data
	return nil
}

func NewRawAction(msg string) (RawAction, error) {
	var rawAction RawAction
	err := json.Unmarshal([]byte(msg), &rawAction)
	if err != nil {
		return rawAction, err
	}
	return rawAction, nil
}

func (ra *RawAction) ToInitAction() (InitAction, error) {
	var a InitAction
	if ra.ActionType != "initialize" {
		return a, fmt.Errorf("raw action type <%s> cannot be converted to InitAction", ra.ActionType)	
	}
	err := json.Unmarshal(ra.Raw, &a)
	if err != nil {
		return a, err
	}
	return a, nil
}
func (ra *RawAction) ToProcessAction() (ProcessAction, error) {
	var a ProcessAction
	if ra.ActionType != "processRecords" {
		return a, fmt.Errorf("raw action type <%s> cannot be converted to ProcessAction", ra.ActionType)	
	}
	err := json.Unmarshal(ra.Raw, &a)
	if err != nil {
		return a, err
	}
	return a, nil
}
func (ra *RawAction) ToLeaseLostAction() (LeaseLostAction, error) {
	var a LeaseLostAction
	if ra.ActionType != "leastLost" {
		return a, fmt.Errorf("raw action type <%s> cannot be converted to LeastLostAction", ra.ActionType)	
	}
	err := json.Unmarshal(ra.Raw, &a)
	if err != nil {
		return a, err
	}
	return a, nil
}
func (ra *RawAction) ToShardEndedAction() (ShardEndedAction, error) {
	var a ShardEndedAction
	if ra.ActionType != "shardEnded" {
		return a, fmt.Errorf("raw action type <%s> cannot be converted to ShardEndedAction", ra.ActionType)	
	}
	err := json.Unmarshal(ra.Raw, &a)
	if err != nil {
		return a, err
	}
	return a, nil
}
func (ra *RawAction) ToShutdownRequestedAction() (ShutdownRequestedAction, error) {
	var a ShutdownRequestedAction
	if ra.ActionType != "shutdownRequested" {
		return a, fmt.Errorf("raw action type <%s> cannot be converted to ShutDownRequestedAction", ra.ActionType)	
	}
	err := json.Unmarshal(ra.Raw, &a)
	if err != nil {
		return a, err
	}
	return a, nil
}

type InitAction struct {
	Action    string `json:"action"`
	ShardId   string `json:"shardId"`
	SeqNum    string `json:"sequenceNumber"`
	SubSeqNum int `json:"subSequenceNumber"`
}

type ProcessAction struct {
	Action             string       `json:"action"`
	MillisBehindLatest int       `json:"millisBehindLatest"`
	Records            []kcl.Record `json:"records"`
}

type LeaseLostAction struct {
	Action             string       `json:"action"`
}

type ShardEndedAction struct {
	Action             string       `json:"action"`
}

type ShutdownRequestedAction struct {
	Action             string       `json:"action"`
}
