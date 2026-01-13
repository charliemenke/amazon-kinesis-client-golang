package actions

import (
	"encoding/json"
	"fmt"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type Action interface {
	ActionType() string
	Dispatch() error
}

type rawAction struct {
	ActionType string `json:"action"`
	Raw        []byte
}

func (a *rawAction) UnmarshalJSON(data []byte) error {
	type Alias rawAction
	var tmp Alias
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	*a = rawAction(tmp)
	a.Raw = data
	return nil
}

func NewRawAction(msg string) (rawAction, error) {
	var rawAction rawAction
	err := json.Unmarshal([]byte(msg), &rawAction)
	if err != nil {
		return rawAction, err
	}
	return rawAction, nil
}

func (a *rawAction) Decode(rp kcl.RecordProcessor, cp *checkpoint.Checkpointer) (Action, error) {
	switch a.ActionType {
	case "initialize":
		initAction, err := NewInitAction(rp, a.Raw)
		if err != nil {
			return nil, err
		}
		return initAction, nil
	case "shutdownRequested":
		shutdownRequestedAction, err := NewShutdownRequestedAction(rp, cp, a.Raw)
		if err != nil {
			return nil, err
		}
		return shutdownRequestedAction, nil
	case "processRecords":
		processAction, err := NewProcessAction(rp, cp, a.Raw)
		if err != nil {
			return nil, err
		}
		return processAction, nil
	case "leaseLost":
		leaseLostAction, err := NewLeaseLostAction(rp, a.Raw)
		if err != nil {
			return nil, err
		}
		return leaseLostAction, nil
	case "shardEnded":
		shardEndedAction, err := NewShardEndedAction(rp, a.Raw)
		if err != nil {
			return nil, err
		}
		return shardEndedAction, nil
	default:
		return nil, fmt.Errorf("unsupported action type: %s", a.ActionType)
	}
}
