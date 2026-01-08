package kclmanager

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl"
)

type KCLManager struct {
	input           *bufio.Reader
	output          io.Writer
	errOutput       io.Writer
	recordProcessor kcl.RecordProcessor
	checkpointer    *checkpoint.Checkpointer
	loggr           *slog.Logger
}

func NewKCLManager(input io.Reader, output, errOutput io.Writer, rp kcl.RecordProcessor) *KCLManager {
	return &KCLManager{
		input:           bufio.NewReader(input),
		output:          output,
		errOutput:       output,
		recordProcessor: rp,
		checkpointer:    checkpoint.NewCheckpointer(bufio.NewReader(input), output),
		loggr:           slog.Default(),
	}
}

func (kcl *KCLManager) readAction() (actions.Action, error) {
	in, err := kcl.input.ReadString('\n')
	if err != nil {
		return nil, err
	}
	kcl.loggr.Debug("read raw kcl action", "action", in)

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

func (kcl *KCLManager) processAction(a actions.Action) error {
	// kcl.loggr.Debug("got kcl action", "action_type", in.ActionType())
	err := a.Dispatch(kcl.recordProcessor)
	if err != nil {
		return err
	}
	return nil
}

func (kcl *KCLManager) reportActionDone() {}

func (kcl *KCLManager) Run() {
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
