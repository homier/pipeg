package pipeg

import (
	"context"
	"errors"
	"log/slog"

	"github.com/cenkalti/backoff/v5"
)

type Pipeline[T any] struct {
	Name     string
	Stages   []Stage[T]
	Logger   *slog.Logger
	Metricer Metricer
}

type Config struct {
	Logger   slog.Logger
	Metricer Metricer
}

func NewPipeline[T any](
	name string,
	config Config,
	stages ...Stage[T],
) *Pipeline[T] {
	p := &Pipeline[T]{
		Name:     name,
		Stages:   make([]Stage[T], len(stages)),
		Logger:   config.Logger.With(slog.String("pipeline", name)),
		Metricer: config.Metricer,
	}

	for idx, stage := range stages {
		p.Stages[idx] = stage.WithLogger(p.Logger.With(
			"stage", stage.Config().Name,
		))
	}

	return p
}

func (p *Pipeline[T]) Process(ctx context.Context, entry T) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.Logger.Debug("starting entry processing")
	defer p.Logger.Debug("entry processing completed")

	if p.Metricer != nil {
		observer := p.Metricer.PipelineTimer(p.Name)
		defer observer.ObserveDuration()

		defer func() {
			if err != nil {
				p.Metricer.IncPipelineFailed(p.Name)
				return
			}

			p.Metricer.IncPipelineProcessed(p.Name)
		}()
	}

	for _, stage := range p.Stages {
		if err := p.processStage(ctx, stage, entry); err != nil {
			if errors.Is(err, ErrBreak) {
				return nil
			}

			return err
		}
	}

	return nil
}

func (p *Pipeline[T]) processStage(ctx context.Context, stage Stage[T], entry T) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	config := stage.Config()
	if !config.Enabled {
		return nil
	}

	if p.Metricer != nil {
		observer := p.Metricer.StageTimer(p.Name, config.Name)
		defer observer.ObserveDuration()

		defer func() {
			if err != nil {
				p.Metricer.IncStageFailed(p.Name, config.Name)
				return
			}

			p.Metricer.IncStageProcessed(p.Name, config.Name)
		}()
	}

	if config.Retry == nil {
		return stage.Process(ctx, entry)
	}

	_, err = backoff.Retry(
		ctx, func() (any, error) {
			return nil, stage.Process(ctx, entry)
		},
		backoff.WithBackOff(p.getStageBackoff(config.Name, config.Retry)),
		backoff.WithMaxTries(config.Retry.MaxAttempts),
		backoff.WithMaxElapsedTime(config.Retry.MaxElapsedTime),
	)

	return err
}

func (p *Pipeline[T]) getStageBackoff(name string, config *StageRetry) backoff.BackOff {
	switch config.Policy {
	case StageRetryPolicyExponential:
		b := backoff.NewExponentialBackOff()
		b.MaxInterval = config.MaxInterval
		b.RandomizationFactor = config.RandomizationFactor
		b.Multiplier = config.Multiplier

		return b
	case StageRetryPolicyConstant:
		return backoff.NewConstantBackOff(config.MaxInterval)
	case StageRetryPolicyImmediate:
		return &backoff.ZeroBackOff{}
	default:
		p.Logger.Debug(
			"unknown stage retry policy, exponential will be used",
			slog.String("stage", name),
			slog.Int("retry_policy", int(config.Policy)),
		)

		b := backoff.NewExponentialBackOff()
		b.MaxInterval = config.MaxInterval
		b.RandomizationFactor = config.RandomizationFactor
		b.Multiplier = config.Multiplier

		return b
	}
}
