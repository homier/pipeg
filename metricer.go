package pipeg

type Metricer interface {
	IncPipelineFailed(pipeline string, reason string)
	IncPipelineProcessed(pipeline string)
	IncPipelineBreak(pipeline, stage, reason string)

	IncStageFailed(pipeline, stage string, reason string)
	IncStageProcessed(pipeline, stage string)

	PipelineTimer(pipeline string) DurationObserver
	StageTimer(pipeline, stage string) DurationObserver
}

type DurationObserver interface {
	ObserveDuration()
}
