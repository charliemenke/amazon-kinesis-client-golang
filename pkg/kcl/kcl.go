package kcl

type RecordProcessor interface {
	Initialize() error
	ProcessRecords(records []string)
	Shutdown(reason string)
}
