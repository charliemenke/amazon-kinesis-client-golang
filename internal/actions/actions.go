package actions

import (
	"encoding/json"
	"fmt"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type Action interface {
	ActionType() string
	Dispatch(rp kcl.RecordProcessor) error
}

type rawAction struct {
	ActionType	string `json:"action"`
	Raw			[]byte	
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

func (a *rawAction) Decode() (Action, error) {
	switch a.ActionType {
	case "initialize":
		var initAction InitAction
		err := json.Unmarshal(a.Raw, &initAction)
		if err != nil {
			return nil, err
		}
		return &initAction, nil
	case "shutdown":
	case "shutdownRequested":
	case "processRecords":
	case "checkpoint":
	case "leaseLost":
	case "shardEnded":
	default:
		return nil, fmt.Errorf("unsupported action type: %s", a.ActionType)
	}
	return nil, nil
}
