package kcl

import (
	"bufio"
	"io"
	"log/slog"
)

type RecordProcessor interface {
	Initialize() error
	ProcessRecords(records []string)
	Shutdown(reason string)
}

type Action interface {
	ActionType() string
	dispatch(rp RecordProcessor) error
}

type KCLProcess struct {
	input *bufio.Reader
	output io.Writer
	errOutput io.Writer
	recordProcessor	RecordProcessor
	loggr *slog.Logger
}

func NewKCLProcess(input io.Reader, output, errOutput io.Writer, rp RecordProcessor) *KCLProcess {
	return &KCLProcess{
		input: bufio.NewReader(input),
		output: output,
		errOutput: output,
		recordProcessor: rp,
	}
}

func (kcl *KCLProcess) readAction() (Action, error) {
	in, err := kcl.input.ReadString('\n')
	kcl.loggr.Debug("read raw kcl action", "action", in)
	if err != nil {
		return nil, err
	}
	// action, err := NewKCLAction(kcl.input.Text())
	// if err != nil {
	// 	return nil, err
	// }
	return nil, nil
}

func (kcl *KCLProcess) processAction(a Action) error {
	// kcl.loggr.Debug("got kcl action", "action_type", in.ActionType())
	err := a.dispatch(kcl.recordProcessor)
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
