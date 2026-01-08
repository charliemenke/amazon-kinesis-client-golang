package actions

import "github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"

type InitAction struct {
	Action    string `json:"action"`
	ShardId   string `json:"shardId"`
	SeqNum    string `json:"sequenceNumber"`
	SubSeqNum string `json:"subSequenceNumber"`
}

func (a *InitAction) ActionType() string {
	return a.ActionType()
}

func (a *InitAction) Dispatch(rp kcl.RecordProcessor) error {
	err := rp.Initialize(a.ShardId, a.SeqNum, a.SubSeqNum)
	if err != nil {
		return err
	}
	return nil
}
