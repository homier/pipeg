package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/cenkalti/backoff/v5"

	"github.com/homier/pipeg"
)

type Message struct {
	Data []byte `json:"data"`
}

type ValidateMessageRetrier struct {
}

func (r *ValidateMessageRetrier) Retry(ctx context.Context, f func() (any, error)) error {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     time.Millisecond * 5,
		MaxInterval:         time.Second,
		Multiplier:          backoff.DefaultMultiplier,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
	}

	_, err := backoff.Retry(
		ctx, f,
		backoff.WithBackOff(b),
		backoff.WithMaxTries(3),
	)

	return err
}

func ValidateMessage(ctx context.Context, _ *slog.Logger, message *Message) error {
	if message.Data == nil {
		message.Data = []byte("data")

		return errors.New("data must not be empty")
	}

	return nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	pipe := pipeg.New(
		"retries",
		pipeg.Config{Logger: logger},
		pipeg.NewStageRetryable("validator", ValidateMessage, pipeg.StageConfig{}, &ValidateMessageRetrier{}),
	)

	message := &Message{}
	if err := pipe.Process(context.Background(), message); err != nil {
		panic(err)
	}

	logger.Info("pipeline completed", slog.String("data", string(message.Data)))
}
