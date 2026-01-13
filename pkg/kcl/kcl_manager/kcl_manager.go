package kclmanager

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

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
		loggr:           slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
}

func (kcl *KCLManager) readAction() (actions.Action, error) {
	in, err := kcl.input.ReadString('\n')
	if err != nil {
		return nil, err
	}
	kcl.loggr.Debug("read raw kcl action", "action", in)

	rawAction, err := actions.NewRawAction(in)
	if err != nil {
		return nil, fmt.Errorf("error reading kcl input action %s: %v", in, err)
	}

	action, err := rawAction.Decode(kcl.recordProcessor, kcl.checkpointer)
	if err != nil {
		return nil, err
	}

	return action, nil
}

func (kcl *KCLManager) processAction(a actions.Action) error {
	kcl.loggr.Debug("got kcl action", "action_type", a.ActionType())
	err := a.Dispatch()
	if err != nil {
		return err
	}
	return nil
}

func (kcl *KCLManager) reportActionDone(actionType string) error {
	output := map[string]string{"action": "status", "responseFor": actionType}
	encoder := json.NewEncoder(kcl.output)
	err := encoder.Encode(output)
	if err != nil {
		return err
	}
	return nil
}

func (kcl *KCLManager) Run() {
	kcl.loggr.Info("starting up kcl interface, waiting for first instruction...")
	for {
		action, err := kcl.readAction()
		if err != nil {
			panic(err)
		}
		err = kcl.processAction(action)
		if err != nil {
			panic(err)
		}
		err = kcl.reportActionDone(action.ActionType())
		if err != nil {
			panic(err)
		}
	}
}
