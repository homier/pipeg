package pipeg

import (
	"context"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type Stager[T any] interface {
	SetLogger(logger *slog.Logger)
	Config() StageConfig

	Execute(ctx context.Context, entry T) error
}

type Stage[T any] struct {
	Cfg    StageConfig
	Logger *slog.Logger

	Do StageDo[T]
}

type StageDo[T any] func(ctx context.Context, logger *slog.Logger, entry T) error

var _ Stager[any] = (*Stage[any])(nil)

func NewStage[T any](doFunc StageDo[T], config StageConfig) *Stage[T] {
	return &Stage[T]{Cfg: config, Do: doFunc}
}

// Config implements Stager.
func (s *Stage[T]) Config() StageConfig {
	return s.Cfg
}

// SetLogger implements Stager.
func (s *Stage[T]) SetLogger(logger *slog.Logger) {
	s.Logger = logger
}

// Execute implements Stager.
func (s *Stage[T]) Execute(ctx context.Context, entry T) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return s.Do(ctx, s.Logger, entry)
}

type StageConfig struct {
	Name     string `json:"name"`
	Disabled bool   `json:"disabled"`

	Timeout time.Duration `json:"timeout"`

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
