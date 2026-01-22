package checkpoint

import (
	"encoding/json"
	"fmt"
	"io"
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
}

func NewCheckpointer(input io.Reader, output io.Writer) *Checkpointer {
	return &Checkpointer{
		input:  json.NewDecoder(input),
		output: json.NewEncoder(output),
	}
}

func (c *Checkpointer) checkKCLResp() error {
	var resp checkPointResp
	err := c.input.Decode(&resp)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("bad checkpoint ack from kcl multilang process: %s", resp.Error)
	}
	return nil
}

func (c *Checkpointer) CheckpointBatch() error {
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
