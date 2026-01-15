package actions

import (
	"encoding/json"
	"fmt"

)

const (
	INITITALIZE = "initalize"
	PROCESS_RECORDS = "processRecords"
	LEASE_LOST = "leaseLost"
	SHARD_ENDED = "shardEnded"
	SHUTDOWN_REQUESTED = "shutdownRequested"
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
	if ra.ActionType != INITITALIZE {
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
	if ra.ActionType != PROCESS_RECORDS {
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
	if ra.ActionType != LEASE_LOST {
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
	if ra.ActionType != SHARD_ENDED {
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
	if ra.ActionType != SHUTDOWN_REQUESTED {
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
	SubSeqNum int    `json:"subSequenceNumber"`
}

type ProcessAction struct {
	Action             string       `json:"action"`
	MillisBehindLatest int          `json:"millisBehindLatest"`
	Records            []Record `json:"records"`
}

// Record defines the json shape of a Kinesis "record" as
// it comes over in the KCL Multilang process action
// ProcessRecords request.
type Record struct {
	Action                      string `json:"action"`
	Data                        string `json:"data"`
	PartitionKey                string `json:"partitionKey"`
	ApproximateArrivalTimestamp int    `json:"approximateArrivalTimestamp"`
	SequenceNumber              string `json:"sequenceNumber"`
	SubSequenceNumber           int    `json:"subSequenceNumber"`
}


type LeaseLostAction struct {
	Action string `json:"action"`
}

type ShardEndedAction struct {
	Action string `json:"action"`
}

type ShutdownRequestedAction struct {
	Action string `json:"action"`
}
