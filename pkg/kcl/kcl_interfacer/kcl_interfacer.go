package kclinterfacer

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/charliemenke/amazon-kinesis-client-golang/internal/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/internal/checkpoint"
)

// KCLInterface is the interface in which you can communicate
// with a the KCL Multilang process. It includes methods needed
// to read and write actions/requests, as well as a Checkpointer
// to allow you to checkpoint your consumption progress.
type KCLInterface struct {
	input *json.Decoder
	output *json.Encoder
	loggr *slog.Logger
	Checkpointer	*checkpoint.Checkpointer
}

type KCLInterfaceOpts func(kclI *KCLInterface)

func NewKCLInterface(i io.Reader, o io.Writer, opts... KCLInterfaceOpts) *KCLInterface {
	kcli := &KCLInterface{
		input: json.NewDecoder(os.Stdin),
		output: json.NewEncoder(os.Stdout),
		loggr: slog.Default(),
	}
	for _, opt := range opts {
		opt(kcli)
	}
	kcli.Checkpointer = checkpoint.NewCheckpointer(i, o, kcli.loggr)
	return kcli
}

func WithLogger(l *slog.Logger) KCLInterfaceOpts {
	return func(kcli *KCLInterface) {
		kcli.loggr = l
	}
}

// ReadActionRequest reads the next available KCL Multilang action
// request. KCL Multilang sends its action requests over stdout in the
// form of json. Please see `internal/actions/` for a list of potential
// KCL actions requested.
func (kcli *KCLInterface) ReadActionRequest() (actions.RawAction, error) {
	var rawAction actions.RawAction
	err := kcli.input.Decode(&rawAction)
	if err != nil {
		return rawAction, fmt.Errorf("error reading kcl action request: %v", err)
	}
	kcli.loggr.Debug("kcl multilang raw action read", "action_type", rawAction.ActionType)

	return rawAction, nil
}

// WriteActionComplete writes the expected json response to KCL Multilang
// process to the output. KCL Multilang expects a json formated response
// sent to stdout in the following format after every successfull action:
//   { "action": "status", "responseFor": "<action type you completed>" }
func (kcli *KCLInterface) WriteActionComplete(actionType string) error {
	kcli.loggr.Debug("reporting success status back to kcl multilang process", "response_for", actionType)
	output := map[string]string{"action": "status", "responseFor": actionType}
	err := kcli.output.Encode(output)
	if err != nil {
		return err
	}
	return nil
}
