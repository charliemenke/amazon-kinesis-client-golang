package checkpoint

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
)

type checkPointResp struct {
	Action            string `json:"action"`
	SequenceNumber    string `json:"sequenceNumber"`
	SubSequenceNumber string `json:"subSequenceNumber"`
	Error             string `json:"error"`
}

type Checkpointer struct {
	input  *json.Decoder
	output *json.Encoder
	loggr  *slog.Logger
}

func NewCheckpointer(input io.Reader, output io.Writer, loggr *slog.Logger) *Checkpointer {
	return &Checkpointer{
		input:  json.NewDecoder(input),
		output: json.NewEncoder(output),
		loggr:  loggr,
	}
}

func (c *Checkpointer) checkKCLResp() error {
	var resp checkPointResp
	err := c.input.Decode(&resp)
	if err != nil {
		return err
	}
	c.loggr.Debug("recieved checkpoint response from kcl multilang process", "response", resp)

	if resp.Error != "" {
		return fmt.Errorf("bad checkpoint ack from kcl multilang process: %s", resp.Error)
	}
	return nil
}

func (c *Checkpointer) CheckpointBatch() error {
	c.loggr.Debug("got checkpoint batch request from record processor")
	output := map[string]string{"action": "checkpoint"}
	err := c.output.Encode(output)
	if err != nil {
		return err
	}

	err = c.checkKCLResp()
	if err != nil {
		return err
	}
	return nil
}
func (c *Checkpointer) CheckpointSeqNum(seqNum string) error {
	c.loggr.Debug("got checkpoint seq num request from record processor", "seq_num", seqNum)
	output := map[string]any{
		"action":         "checkpoint",
		"sequenceNumber": seqNum,
	}
	err := c.output.Encode(output)
	if err != nil {
		return err
	}

	err = c.checkKCLResp()
	if err != nil {
		return err
	}
	return nil
}
func (c *Checkpointer) CheckpointSubSeqNum(seqNum string, subSeqNum int) error {
	c.loggr.Debug("got checkpoint seq num with sub seq num request from record processor", "seq_num", seqNum, "sub_seq_num", subSeqNum)
	output := map[string]any{
		"action":            "checkpoint",
		"sequenceNumber":    seqNum,
		"subSequenceNumber": subSeqNum,
	}
	err := c.output.Encode(output)
	if err != nil {
		return err
	}

	err = c.checkKCLResp()
	if err != nil {
		return err
	}
	return nil
}
