package checkpoint

import (
	"bufio"
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
	input  *bufio.Reader
	output io.Writer
}

func NewCheckpointer(input *bufio.Reader, output io.Writer) *Checkpointer {
	return &Checkpointer{input: input, output: output}
}

func (c *Checkpointer) CheckKCLResp() error {
	in, err := c.input.ReadString('\n')
	if err != nil {
		return err
	}

	var resp checkPointResp
	err = json.Unmarshal([]byte(in), &resp)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("bad checkpoint ack from kcl: %s\n", err)
	}
	return nil
}

func (c *Checkpointer) CheckpointBatch() error {
	output := map[string]string{"action": "checkpoint"}
	encoder := json.NewEncoder(c.output)
	err := encoder.Encode(output)
	if err != nil {
		return err
	}

	err = c.CheckKCLResp()
	if err != nil {
		return err
	}
	return nil
}
func (c *Checkpointer) CheckpointSeqNum(seqNum int) error {
	output := map[string]any{
		"action":         "checkpoint",
		"sequenceNumber": seqNum,
	}
	encoder := json.NewEncoder(c.output)
	err := encoder.Encode(output)
	if err != nil {
		return err
	}

	err = c.CheckKCLResp()
	if err != nil {
		return err
	}
	return nil
}
func (c *Checkpointer) CheckpointSubSeqNum(seqNum, subSeqNum int) error {
	output := map[string]any{
		"action":            "checkpoint",
		"sequenceNumber":    seqNum,
		"subSequenceNumber": subSeqNum,
	}
	encoder := json.NewEncoder(c.output)
	err := encoder.Encode(output)
	if err != nil {
		return err
	}

	err = c.CheckKCLResp()
	if err != nil {
		return err
	}
	return nil
}
