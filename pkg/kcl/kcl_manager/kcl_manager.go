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

type KCLManagerOpts func(kclm *KCLManager)

func NewKCLManager(rp kcl.RecordProcessor, opts... KCLManagerOpts) *KCLManager {
	kclm := &KCLManager{
		input:           bufio.NewReader(os.Stdin),
		output:          os.Stdout,
		errOutput:       os.Stderr,
		recordProcessor: rp,
		loggr:           slog.Default(),
	}
	for _, opt := range opts {
		opt(kclm)
	}
	// set checkpointer after apply opts since user could spec differ inputs and outputs
	kclm.checkpointer = checkpoint.NewCheckpointer(kclm.input, kclm.output, kclm.loggr)
	return kclm
}

func WithLogger(l *slog.Logger) KCLManagerOpts {
	return func(kclm *KCLManager) {
		kclm.loggr = l
	}
}

func WithInput(i io.Reader) KCLManagerOpts {
	return func(kclm *KCLManager) {
		kclm.input = bufio.NewReader(i)
	}
}

func WithOutput(o io.Writer) KCLManagerOpts {
	return func(kclm *KCLManager) {
		kclm.output = o
	}
}

func WithErrOutput(eo io.Writer) KCLManagerOpts {
	return func(kclm *KCLManager) {
		kclm.errOutput = eo
	}
}

func (kcl *KCLManager) readAction() (actions.Action, error) {
	in, err := kcl.input.ReadString('\n')
	if err != nil {
		return nil, err
	}
	kcl.loggr.Debug("read raw stdin from kcl multilang", "stdin", in)

	rawAction, err := actions.NewRawAction(in)
	if err != nil {
		return nil, fmt.Errorf("error reading kcl input action %s: %v", in, err)
	}
	kcl.loggr.Debug("kcl multilang action read", "action_type", rawAction.ActionType)

	action, err := rawAction.Decode(kcl.recordProcessor, kcl.checkpointer)
	if err != nil {
		return nil, fmt.Errorf("error decoding raw action into kcl action: %v", err)
	}

	return action, nil
}

func (kcl *KCLManager) processAction(a actions.Action) error {
	kcl.loggr.Debug("processing kcl multilang action request", "action_type", a.ActionType())
	err := a.Dispatch()
	if err != nil {
		return fmt.Errorf("error processing kcl multilang action request: %v", err)
	}
	return nil
}

func (kcl *KCLManager) reportActionDone(actionType string) error {
	kcl.loggr.Debug("reporting success status back to kcl multilang process", "response_for", actionType)
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
		kcl.loggr.Debug("waiting for next kcl multilang input request")
	}
}
