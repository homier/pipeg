package pipeg

import (
	"context"
	"log/slog"

	"github.com/cenkalti/backoff/v5"
)

type Pipeline[T any] struct {
	Name     string
	Stages   []Stager[T]
	Logger   *slog.Logger
	Metricer Metricer
}

type Config struct {
	Logger   *slog.Logger
	Metricer Metricer
}

func New[T any](
	name string,
	config Config,
	stages ...Stager[T],
) *Pipeline[T] {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	p := &Pipeline[T]{
		Name:     name,
		Stages:   make([]Stager[T], len(stages)),
		Logger:   config.Logger.With(slog.String("pipeline", name)),
		Metricer: config.Metricer,
	}

	for idx, stage := range stages {
		stage.SetLogger(p.Logger.With("stage", stage.Config().Name))

		p.Stages[idx] = stage
	}

	return p
}

func (p *Pipeline[T]) Process(ctx context.Context, entry T) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.Logger.Debug("starting entry processing")
	defer p.Logger.Debug("entry processing completed")

	pipelineBreak := false

	if p.Metricer != nil {
		observer := p.Metricer.PipelineTimer(p.Name)
		defer observer.ObserveDuration()

		defer func() {
			if err != nil {
				p.Metricer.IncPipelineFailed(p.Name)
				return
			}

			if !pipelineBreak {
				p.Metricer.IncPipelineProcessed(p.Name)
			}
		}()
	}

	for _, stage := range p.Stages {
		if err := p.processStage(ctx, stage, entry); err != nil {
			if err, ok := IsBreak(err); ok {
				pipelineBreak = true

				p.Logger.Debug(
					"breaking from pipeline upong stage request",
					slog.String("error", err.Error()),
					slog.String("stage", stage.Config().Name),
				)

				if p.Metricer != nil {
					p.Metricer.IncPipelineBreak(p.Name, stage.Config().Name, err.Reason)
				}

				return nil
			}

			return err
		}
	}

	return nil
}

func (p *Pipeline[T]) processStage(ctx context.Context, stage Stager[T], entry T) (err error) {
	config := stage.Config()
	if config.Disabled {
		return nil
	}

	var cancel context.CancelFunc
	if config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	defer cancel()

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
			if err := stage.Process(ctx, entry); err != nil {
				if _, ok := IsBreak(err); ok {
					return nil, backoff.Permanent(err)
				}

				return nil, err
			}

			return nil, nil
		},
		backoff.WithBackOff(p.getStageBackoff(config.Name, config.Retry)),
		backoff.WithMaxTries(config.Retry.MaxAttempts),
		backoff.WithMaxElapsedTime(config.Retry.MaxElapsedTime),
	)

	if err == nil {
		return nil
	}

	if err, ok := err.(*backoff.PermanentError); ok {
		return err.Unwrap()
	}

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
