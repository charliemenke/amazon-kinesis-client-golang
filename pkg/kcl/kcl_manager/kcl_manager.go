package kclmanager

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type KCLProcess struct {
	input           *bufio.Reader
	output          io.Writer
	errOutput       io.Writer
	recordProcessor kcl.RecordProcessor
	loggr           *slog.Logger
}

func NewKCLProcess(input io.Reader, output, errOutput io.Writer, rp kcl.RecordProcessor) *KCLProcess {
	return &KCLProcess{
		input:           bufio.NewReader(input),
		output:          output,
		errOutput:       output,
		recordProcessor: rp,
	}
}

func (kcl *KCLProcess) readAction() (actions.Action, error) {
	in, err := kcl.input.ReadString('\n')
	kcl.loggr.Debug("read raw kcl action", "action", in)
	if err != nil {
		return nil, err
	}
	rawAction, err := actions.NewRawAction(in)
	switch rawAction.ActionType {
	case "initialize":
	case "shutdown":
	case "shutdownRequested":
	case "processRecords":
	case "checkpoint":
	case "leaseLost":
	case "shardEnded":
	}
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (kcl *KCLProcess) processAction(a actions.Action) error {
	// kcl.loggr.Debug("got kcl action", "action_type", in.ActionType())
	err := a.Dispatch(kcl.recordProcessor)
	if err != nil {
		return err
	}
	return nil
}

func (kcl *KCLProcess) reportActionDone() {}

func (kcl *KCLProcess) Run() {
	for {
		action, err := kcl.readAction()
		if err != nil {

		}
		err = kcl.processAction(action)
		if err != nil {

		}
		kcl.reportActionDone()
	}
}
