package kcl

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/checkpoint"
)

// MultilangInterface is the interface in which you can communicate
// with a the KCL Multilang process. It includes methods needed
// to read and write actions/requests, as well as a Checkpointer
// to allow you to checkpoint your consumption progress.
type MultilangInterface struct {
	input        *json.Decoder
	output       *json.Encoder
	Checkpointer *checkpoint.Checkpointer
}

type MultilangInterfaceOpts func(mli *MultilangInterface)

func NewMultilangInterface(i io.Reader, o io.Writer, opts ...MultilangInterfaceOpts) *MultilangInterface {
	kcli := &MultilangInterface{
		input:  json.NewDecoder(i),
		output: json.NewEncoder(o),
	}
	for _, opt := range opts {
		opt(kcli)
	}
	kcli.Checkpointer = checkpoint.NewCheckpointer(i, o)
	return kcli
}

// ReadActionRequest reads the next available KCL Multilang action
// request. KCL Multilang sends its action requests over stdout in the
// form of json. Please see `internal/actions/` for a list of potential
// KCL actions requested.
func (kcli *MultilangInterface) ReadActionRequest() (actions.RawAction, error) {
	var rawAction actions.RawAction
	err := kcli.input.Decode(&rawAction)
	if err != nil {
		return rawAction, fmt.Errorf("error reading kcl action request: %v", err)
	}

	return rawAction, nil
}

// WriteActionComplete writes the expected json response to KCL Multilang
// process to the output. KCL Multilang expects a json formated response
// sent to stdout in the following format after every successfull action:
//
//	{ "action": "status", "responseFor": "<action type you completed>" }
func (kcli *MultilangInterface) WriteActionComplete(actionType string) error {
	output := map[string]string{"action": "status", "responseFor": actionType}
	err := kcli.output.Encode(output)
	if err != nil {
		return err
	}
	return nil
}
