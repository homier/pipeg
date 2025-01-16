package pipeg

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type Stager[T any] interface {
	SetLogger(logger *slog.Logger)
	Config() StageConfig

	Process(ctx context.Context, entry T) error
}

type Stage[T any] struct {
	Cfg    StageConfig
	Logger *slog.Logger
}

var _ Stager[any] = (*Stage[any])(nil)

// Config implements Stager.
func (s *Stage[T]) Config() StageConfig {
	return s.Cfg
}

// Process implements Stager.
func (s *Stage[T]) Process(ctx context.Context, entry T) error {
	return errors.New("not implemented")
}

// SetLogger implements Stager.
func (s *Stage[T]) SetLogger(logger *slog.Logger) {
	s.Logger = logger
}

type StageConfig struct {
	Name     string `json:"name"`
	Disabled bool   `json:"disabled"`

	Retry *StageRetry `json:"retry"`
}

type StageRetry struct {
	Policy              StageRetryPolicy `json:"stage_retry_policy"`
	MaxAttempts         uint             `json:"max_attempts"`
	MaxElapsedTime      time.Duration    `json:"max_elapsed_time"`
	MaxInterval         time.Duration    `json:"max_interval"`
	RandomizationFactor float64          `json:"randomization_factor"`
	Multiplier          float64          `json:"multiplier"`
}

type StageRetryPolicy int

const (
	StageRetryPolicyExponential StageRetryPolicy = iota
	StageRetryPolicyConstant
	StageRetryPolicyImmediate
)

const (
	StageRetryDefaultPolicy              = StageRetryPolicyExponential
	StageRetryDefaultMaxAttempts         = uint(0)
	StageRetryDefaultMaxElapsedTime      = time.Second * 15
	StageRetryDefaultMaxInterval         = time.Second * 5
	StageRetryDefaultRandomizationFactor = backoff.DefaultRandomizationFactor
	StageRetryDefaultMultiplier          = backoff.DefaultMultiplier
)

func StageRetryDefault() *StageRetry {
	return &StageRetry{
		Policy:              StageRetryDefaultPolicy,
		MaxAttempts:         StageRetryDefaultMaxAttempts,
		MaxElapsedTime:      StageRetryDefaultMaxElapsedTime,
		MaxInterval:         StageRetryDefaultMaxInterval,
		RandomizationFactor: StageRetryDefaultRandomizationFactor,
		Multiplier:          StageRetryDefaultMultiplier,
	}
}
