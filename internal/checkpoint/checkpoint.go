package checkpoint

import (
	"bufio"
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
	input  *bufio.Reader
	output io.Writer
	loggr *slog.Logger
}

func NewCheckpointer(input *bufio.Reader, output io.Writer, loggr *slog.Logger) *Checkpointer {
	return &Checkpointer{input: input, output: output, loggr: loggr}
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
	c.loggr.Debug("recieved checkpoint response from kcl multilang process", "response", resp)

	if resp.Error != "" {
		return fmt.Errorf("bad checkpoint ack from kcl multilang process: %s\n", resp.Error)
	}
	return nil
}

func (c *Checkpointer) CheckpointBatch() error {
	c.loggr.Debug("got checkpoint batch request from record processor")
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
func (c *Checkpointer) CheckpointSeqNum(seqNum string) error {
	c.loggr.Debug("got checkpoint seq num request from record processor", "seq_num", seqNum)
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
func (c *Checkpointer) CheckpointSubSeqNum(seqNum string, subSeqNum int) error {
	c.loggr.Debug("got checkpoint seq num with sub seq num request from record processor", "seq_num", seqNum, "sub_seq_num", subSeqNum)
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
