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

	Verbose bool
}

type Config struct {
	Logger   *slog.Logger
	Metricer Metricer
	Verbose  bool
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
		Verbose:  config.Verbose,
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

	p.logVerbose(func() { p.Logger.Debug("starting entry processing") })
	defer p.logVerbose(func() { p.Logger.Debug("entry processing completed") })

	pipelineBreak := false
	if p.Metricer != nil {
		observer := p.Metricer.PipelineTimer(p.Name)
		defer observer.ObserveDuration()

		defer func() {
			if err != nil {
				reason := "internal"
				if err, ok := IsReason(err); ok {
					reason = err.Reason
				}

				p.Metricer.IncPipelineFailed(p.Name, reason)
				return
			}

			if !pipelineBreak {
				p.Metricer.IncPipelineProcessed(p.Name)
			}
		}()
	}

	for _, stage := range p.Stages {
		if err := p.executeStage(ctx, stage, entry); err != nil {
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

func (p *Pipeline[T]) executeStage(ctx context.Context, stage Stager[T], entry T) (err error) {
	config := stage.Config()
	logger := p.Logger.With(slog.String("stage", config.Name))

	if config.Disabled {
		p.logVerbose(func() { logger.Debug("stage is disabled, skipping") })

		return nil
	}

	p.logVerbose(func() { logger.Debug("processing stage") })
	defer func() {
		if err != nil {
			logger.Error("stage executing has failed", slog.String("error", err.Error()))

			return
		}

		p.logVerbose(func() { logger.Debug("stage executing completed") })
	}()

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
				reason := "internal"
				if err, ok := IsReason(err); ok {
					reason = err.Reason
				}

				p.Metricer.IncStageFailed(p.Name, config.Name, reason)
				return
			}

			p.Metricer.IncStageProcessed(p.Name, config.Name)
		}()
	}

	if config.Retry == nil {
		return stage.Execute(ctx, entry)
	}

	_, err = backoff.Retry(
		ctx, func() (any, error) {
			if err := stage.Execute(ctx, entry); err != nil {
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
	exponential := func() backoff.BackOff {
		b := backoff.NewExponentialBackOff()
		b.MaxInterval = config.MaxInterval
		b.RandomizationFactor = config.RandomizationFactor
		b.Multiplier = config.Multiplier

		return b
	}

	switch config.Policy {
	case StageRetryPolicyExponential:
		return exponential()
	case StageRetryPolicyConstant:
		return backoff.NewConstantBackOff(config.MaxInterval)
	case StageRetryPolicyImmediate:
		return &backoff.ZeroBackOff{}
	default:
		p.Logger.Warn(
			"unknown stage retry policy, exponential will be used",
			slog.String("stage", name),
			slog.Int("retry_policy", int(config.Policy)),
		)

		return exponential()
	}
}

func (p *Pipeline[T]) logVerbose(f func()) {
	if p.Verbose {
		f()
	}
}
