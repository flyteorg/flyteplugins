package k8sarray

type State struct {
	currentPhase Phase
}

type Phase uint8

const (
	NotStarted Phase = iota
	MappingFileCreated
	JobSubmitted
	JobsFinished
)

/*
  Discovery for sub-tasks
  Build mapping file
---
  submit jobs (either as a batch or individually)
---BestEffort
  Detect changes to individual job states
    - Check failure ratios
---
  Submit to discovery

*/
