package actions

// type Action interface {
// 	ActionType() string
// 	dispatch(rp RecordProcessor) error
// }

type RawAction struct {
	Action	string	`json:"action"`
	ShardId	string	`json:"shardId,omitempty"`
	Records	any		`json:"records,omitempty"`
}

func NewKCLAction(msg string) {

}
