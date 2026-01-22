package kcl

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/actions"
	"github.com/charliemenke/amazon-kinesis-client-golang/pkg/kcl/checkpoint"
	"github.com/stretchr/testify/assert"
)

// MockRecordProcessor implements the RecordProcessor interface for testing
type MockRecordProcessor struct {
	InitializeCalled        bool
	ProcessRecordsCalled    bool
	LeaseLostCalled         bool
	ShardEndedCalled        bool
	ShutdownRequestedCalled bool

	InitializeError        error
	ProcessRecordsError    error
	LeaseLostError         error
	ShardEndedError        error
	ShutdownRequestedError error

	InitializeArgs struct {
		ShardId   string
		SeqNum    string
		SubSeqNum int
	}

	ProcessRecordsArgs struct {
		Records []actions.Record
		Lag     int
	}
}

func (m *MockRecordProcessor) Initialize(shardId, seqNum string, subSeqNum int) error {
	m.InitializeCalled = true
	m.InitializeArgs.ShardId = shardId
	m.InitializeArgs.SeqNum = seqNum
	m.InitializeArgs.SubSeqNum = subSeqNum
	return m.InitializeError
}

func (m *MockRecordProcessor) ProcessRecords(records []actions.Record, lag int, cp *checkpoint.Checkpointer) error {
	m.ProcessRecordsCalled = true
	m.ProcessRecordsArgs.Records = records
	m.ProcessRecordsArgs.Lag = lag
	return m.ProcessRecordsError
}

func (m *MockRecordProcessor) LeaseLost() error {
	m.LeaseLostCalled = true
	return m.LeaseLostError
}

func (m *MockRecordProcessor) ShardEnded(cp *checkpoint.Checkpointer) error {
	m.ShardEndedCalled = true
	return m.ShardEndedError
}

func (m *MockRecordProcessor) ShutdownRequested(cp *checkpoint.Checkpointer) error {
	m.ShutdownRequestedCalled = true
	return m.ShutdownRequestedError
}

func TestNewManager(t *testing.T) {
	t.Run("creates manager with default logger", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		assert.NotNil(t, manager)
		assert.Equal(t, mockProcessor, manager.recordProcessor)
		assert.NotNil(t, manager.interfacer)
	})

	t.Run("creates manager with custom logger", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)
		logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

		manager := NewManager(mockReader, mockWriter, mockProcessor, WithManagerLogger(logger))

		assert.NotNil(t, manager)
	})
}

func TestProcessRawAction(t *testing.T) {
	t.Run("handles initialize action correctly", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		// Prepare test data
		initAction := actions.InitAction{
			Action:    "initialize",
			ShardId:   "shard-123",
			SeqNum:    "seq-456",
			SubSeqNum: 1,
		}

		actionBytes, _ := json.Marshal(initAction)
		rawAction := actions.RawAction{
			ActionType: "initialize",
			Raw:        actionBytes,
		}

		err := manager.processRawAction(rawAction)
		assert.NoError(t, err)
		assert.True(t, mockProcessor.InitializeCalled)
		assert.Equal(t, "shard-123", mockProcessor.InitializeArgs.ShardId)
		assert.Equal(t, "seq-456", mockProcessor.InitializeArgs.SeqNum)
		assert.Equal(t, 1, mockProcessor.InitializeArgs.SubSeqNum)
	})

	t.Run("handles process records action correctly", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		// write test data to input
		record := actions.Record{
			Action:                      "processRecords",
			Data:                        "test-data",
			PartitionKey:                "key-123",
			ApproximateArrivalTimestamp: 1234567890,
			SequenceNumber:              "seq-1",
			SubSequenceNumber:           0,
		}
		processAction := actions.ProcessAction{
			Action:             "processRecords",
			MillisBehindLatest: 100,
			Records:            []actions.Record{record},
		}
		actionBytes, _ := json.Marshal(processAction)
		mockReader.Write(actionBytes)

		rAction, err := manager.interfacer.ReadActionRequest()
		assert.NoError(t, err)

		err = manager.processRawAction(rAction)
		assert.NoError(t, err)
		assert.True(t, mockProcessor.ProcessRecordsCalled)
		assert.Equal(t, 100, mockProcessor.ProcessRecordsArgs.Lag)
	})

	t.Run("handles lease lost action correctly", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		leaseLostAction := actions.LeaseLostAction{
			Action: "leaseLost",
		}

		actionBytes, _ := json.Marshal(leaseLostAction)
		mockReader.Write(actionBytes)

		rAction, err := manager.interfacer.ReadActionRequest()
		assert.NoError(t, err)

		err = manager.processRawAction(rAction)
		assert.NoError(t, err)
		assert.True(t, mockProcessor.LeaseLostCalled)
	})

	t.Run("handles shard ended action correctly", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		shardEndedAction := actions.ShardEndedAction{
			Action: "shardEnded",
		}

		actionBytes, _ := json.Marshal(shardEndedAction)
		mockReader.Write(actionBytes)

		rAction, err := manager.interfacer.ReadActionRequest()
		assert.NoError(t, err)

		err = manager.processRawAction(rAction)
		assert.NoError(t, err)
		assert.True(t, mockProcessor.ShardEndedCalled)
	})

	t.Run("handles shutdown requested action correctly", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		shutdownRequestedAction := actions.ShutdownRequestedAction{
			Action: "shutdownRequested",
		}

		actionBytes, _ := json.Marshal(shutdownRequestedAction)
		mockReader.Write(actionBytes)

		rAction, err := manager.interfacer.ReadActionRequest()
		assert.NoError(t, err)

		err = manager.processRawAction(rAction)
		assert.NoError(t, err)
		assert.True(t, mockProcessor.ShutdownRequestedCalled)
	})

	t.Run("returns error for unsupported action type", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		unsupportedAction := map[string]string{"action": "I DONT EXIST"}
		actionBytes, _ := json.Marshal(unsupportedAction)
		mockReader.Write(actionBytes)

		rAction, err := manager.interfacer.ReadActionRequest()
		assert.NoError(t, err)

		err = manager.processRawAction(rAction)
		assert.Error(t, err)
	})

	t.Run("handles error from record processor", func(t *testing.T) {
		mockReader := &bytes.Buffer{}
		mockWriter := &bytes.Buffer{}
		mockProcessor := new(MockRecordProcessor)

		manager := NewManager(mockReader, mockWriter, mockProcessor)

		initAction := actions.InitAction{
			Action:    "initialize",
			ShardId:   "shard-123",
			SeqNum:    "seq-456",
			SubSeqNum: 1,
		}

		actionBytes, _ := json.Marshal(initAction)
		mockReader.Write(actionBytes)

		rAction, err := manager.interfacer.ReadActionRequest()
		assert.NoError(t, err)

		mockProcessor.InitializeError = errors.New("processor error")

		err = manager.processRawAction(rAction)
		assert.Error(t, err)
	})
}
