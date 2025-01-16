package pipeg

import (
	"context"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type Stage[T any] interface {
	WithLogger(logger *slog.Logger) Stage[T]
	Config() StageConfig

	Process(ctx context.Context, entry T) error
}

type StageConfig struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`

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
