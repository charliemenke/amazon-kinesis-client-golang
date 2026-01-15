package kcl

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/actions"
)

type Manager struct {
	recordProcessor RecordProcessor
	interfacer      *MultilangInterface
	loggr           *slog.Logger
}

type ManagerOpts func(kclm *Manager)

func NewManager(i io.Reader, o io.Writer, rp RecordProcessor, opts ...ManagerOpts) *Manager {
	kclm := &Manager{
		recordProcessor: rp,
		loggr:           slog.Default(),
	}
	for _, opt := range opts {
		opt(kclm)
	}
	// set interffacer after apply opts since user could spec different logger
	kclm.interfacer = NewMultilangInterface(i, o, WithInterfaceLogger(kclm.loggr))
	return kclm
}

func WithManagerLogger(l *slog.Logger) ManagerOpts {
	return func(kclm *Manager) {
		kclm.loggr = l
	}
}

// processRawAction calls different RecordProcessor methods
// depending on what type of KCL Action the rawAction is.
func (kclm *Manager) processRawAction(ra actions.RawAction) error {
	kclm.loggr.Debug("processing kcl multilang raw action request", "action_type", ra.ActionType)
	// some of this "decoding" of the kcl raw action seems a bit pointless
	// (namely for actions like leastLost) because some of the actions dont
	// actually contain any additional information other than their action
	// name. Regaurdless, this is done to seperate the buisness logic of
	// consuming input from kcl into two steps while retaining seperate
	// action types:
	//     1. Read in stdinput and confirm it is some sort of action (RawAction)
	//     2. Depending on what *type* of action, unmarshal it into its
	//        concrete type and call the relevent record processor method

	// capture recordprossor method error for all cases
	var err error
	switch ra.ActionType {
	case actions.INITITALIZE:
		var a actions.InitAction
		a, err = ra.ToInitAction()
		if err != nil {
			return err
		}
		err = kclm.recordProcessor.Initialize(a.ShardId, a.SeqNum, a.SubSeqNum)
	case actions.SHUTDOWN_REQUESTED:
		// no need to unmarshal to concrete action type
		// since the action contains nothing other than action name
		err = kclm.recordProcessor.ShutdownRequested(kclm.interfacer.Checkpointer)
	case actions.PROCESS_RECORDS:
		var a actions.ProcessAction
		a, err = ra.ToProcessAction()
		if err != nil {
			return err
		}
		err = kclm.recordProcessor.ProcessRecords(a.Records, a.MillisBehindLatest, kclm.interfacer.Checkpointer)
	case actions.LEASE_LOST:
		// no need to unmarshal to concrete action type
		// since the action contains nothing other than action name
		err = kclm.recordProcessor.LeaseLost()
	case actions.SHARD_ENDED:
		// no need to unmarshal to concrete action type
		// since the action contains nothing other than action name
		err = kclm.recordProcessor.ShardEnded(kclm.interfacer.Checkpointer)
	default:
		return fmt.Errorf("unsupported action type: %s", ra.ActionType)
	}
	// handle recordprocessor err if not nil
	if err != nil {
		return err
	}
	return nil
}

// Run is the quickest way to start using this KCL Multilang interface
// to consume kinesis records. It uses an instance of a MultilangInterfacer to
// read Actions requested by the KCL Multilang process, then calls specific
// methods on the provided RecordProcessor depending on which action was
// requested. Finally it uses the interfacer again to write the completed
// status message back to the KCL Multilang process.
func (kclm *Manager) Run() {
	kclm.loggr.Info("starting up kcl interface, waiting for first instruction...")
	for {
		rawAction, err := kclm.interfacer.ReadActionRequest()
		if err != nil {
			panic(err)
		}
		err = kclm.processRawAction(rawAction)
		if err != nil {
			panic(err)
		}
		err = kclm.interfacer.WriteActionComplete(rawAction.ActionType)
		if err != nil {
			panic(err)
		}
		kclm.loggr.Debug("waiting for next kcl multilang input request")
	}
}
