package pipeg

import (
	"context"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type Stager[T any] interface {
	GetConfig() StageConfig
	GetName() string

	Execute(ctx context.Context, entry T) error

	setLogger(logger *slog.Logger)
	setRetrier(retrier Retrier)
}

type Stage[T any] struct {
	Name    string
	Config  StageConfig
	Logger  *slog.Logger
	Retrier Retrier

	Do StageDo[T]
}

type StageDo[T any] func(ctx context.Context, logger *slog.Logger, entry T) error

var _ Stager[any] = (*Stage[any])(nil)

type StageConfig struct {
	Disabled bool `json:"disabled"`

	Timeout time.Duration `json:"timeout"`
}

func NewStage[T any](name string, doFunc StageDo[T], config StageConfig) *Stage[T] {
	return &Stage[T]{Name: name, Config: config, Do: doFunc}
}

func NewStageRetryable[T any](name string, doFunc StageDo[T], config StageConfig, retrier Retrier) *Stage[T] {
	return &Stage[T]{
		Name:    name,
		Config:  config,
		Retrier: retrier,
		Do:      doFunc,
	}
}

// GetName implements Stager.
func (s *Stage[T]) GetName() string {
	return s.Name
}

// GetConfig implements Stager.
func (s *Stage[T]) GetConfig() StageConfig {
	return s.Config
}

// setLogger implements Stager.
func (s *Stage[T]) setLogger(logger *slog.Logger) {
	s.Logger = logger
}

func (s *Stage[T]) setRetrier(retrier Retrier) {
	if s.Retrier == nil {
		s.Retrier = retrier
	}
}

// Execute implements Stager.
func (s *Stage[T]) Execute(ctx context.Context, entry T) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if s.Retrier == nil {
		return s.Do(ctx, s.Logger, entry)
	}

	return s.Retrier.Retry(ctx, func() (any, error) {
		if err := s.Do(ctx, s.Logger, entry); err != nil {
			if _, ok := IsBreak(err); ok {
				return nil, backoff.Permanent(err)
			}

			return nil, err
		}

		return nil, nil
	})
}
