package actions

import (
	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type ProcessAction struct {
	RecordProcessor	kcl.RecordProcessor
	Checkpointer *checkpoint.Checkpointer
	Line ProcessAction	{
		Action             string       `json:"action"`
		MillisBehindLatest string       `json:"millisBehindLatest"`
		Records            []kcl.Record `json:"records"`
	}
}

type recordsWrapper struct {
	
}

func (a *ProcessAction) ActionType() string {
	return a.ActionType()
}

func (a *ProcessAction) Dispatch() error {
	a.RecordProcessor.ProcessRecords(a.Line.Records, a.Checkpointer)
	return nil
}
