package pipeg

type Metricer interface {
	IncPipelineFailed(pipeline string)
	IncPipelineProcessed(pipeline string)

	IncStageFailed(pipeline, stage string)
	IncStageProcessed(pipeline, stage string)

	PipelineTimer(pipeline string) DurationObserver
	StageTimer(pipeline, stage string) DurationObserver
}

type DurationObserver interface {
	ObserveDuration()
}
