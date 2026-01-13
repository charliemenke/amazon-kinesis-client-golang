
# amazon-kinesis-client-golang

A simple wrapper around AWS's Kinesis Client Library, written in Go and modeled based off 
of [amazon-kinesis-client-python](https://github.com/awslabs/amazon-kinesis-client-python). 
Using this library allows the developer to not need to worry about the low level interactions 
between the Java Multilang KCL client and their own record processing code.

## Before You Get Started

It is important to understand that this repo contains two things:

1. **Golang library for KCL Multilang interactions.**

The library and its exported types can be found under

```bash
├── pkg
│   └── kcl
│       ├── kcl_manager
│       │   └── kcl_manager.go
│       └── kcl.go
```

2. **Example usage of said library**

Additionally, also included is a minimal implementation of this library to consume messages. 
This sample application is a combination of these files. Much of which will be described in 
more detail below.

```base
.
├── cmd
│   └── sample
│       ├── sample
│       ├── sample_processor.go
│       └── sample.properties
├── logback.xml
├── Makefile
└─── pom.xml
```

## Using the library

To start consuming Kinesis records with this library you simply need to implement the 
`RecordProcessor` interface found below (a working sample of this can be found under 
`cmd/sample/sample_processor.go`).

```go
type RecordProcessor interface {
	Initialize(shardId, seqNum string, subSeqNum int) error
	ProcessRecords(records []Record, lag int, cp *checkpoint.Checkpointer) error
	LeaseLost() error
	ShardEnded(shardSeqEnd string, cp *checkpoint.Checkpointer) error
	Shutdown(reason string, cp *checkpoint.Checkpointer) error
	ShutdownRequested(cp *checkpoint.Checkpointer) error
}
```

Once you create your implementation of `RecordProcessor`, you can pass it to an instantiation of 
`KCLManager` and call `Run()`.

### Initialize

the `Initialize(...)` method is called exactly once on start up by the kcl multilang process. 
This is a great time to configure any initial state/external dependancies your processor will 
need.

### ProcessRecords

the `ProcessRecords(...)` method is is where your main record processing logic should live. You 
can checkpoint your progress anytime in this method using the provided `Checkpointer` and its 
various checkpointing methods.

> Note: As you process each `Record`, it is smart to keep track of its associated `SequenceNumber` 
so in the case of failure, your processor can checkpoint its progress before shutdown

### LeaseLost

the `LeaseLost(...)` method is called by the kcl multilang process to notify your processor that 
its lease to its Kinesis shard was lost. A checkpointer is not provided for this method as it is 
not advised to checkpoint in this state as another worker most likely already has obtained the 
lost lease.

### ShardEnded

the `ShardEnded(...)` method is primarily called during a re-sharding event in which the shard 
your processor is responsible for is ended and fully drained. You should always checkpoint during 
this event to allow kcl to handle lease cleanup tasks.

### ShutdownRequested

the `ShutdownRequested(...)` method is called when kcl determines the record processor needs to be 
shutdown, either due to the application as a whole being terminated or during a workload 
reshuffle. This method gives your processor a change to checkpoint its progress and perform any 
necessary resource cleanup.

## MVP Implementation

To run an instance of KCL Multilang and configure it to use your golang record processor you 
first need to install the relevant java dependancies. For this example implementation, all 
the setup is controlled by the `Makefile`. Running

```make
make run
```

will download the Java dependancies specified in the `pom.xml` file into `./jars/` as well as 
build the sample record processor implementation found in `cmd/sample/`. Once finished, it will 
start up the Java AWS Multilang process and pass it the build golang binary and ckcl configuration 
file `sample.properties`. The KCL start up process is fairly long so be sure to give it a few 
minutes to spin up.

> Note: since the kcl multilang and your record processor communicate over stdin/out, ensure your 
record processor writes its logs to stderr (if you use log or slog, this is the default output) to 
avoid errors. Additionally, you can edit the `logback.xml` file to tune the kcl multilang process 
logging.
