package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/homier/pipeg"
)

type Message struct {
	Data []byte `json:"data"`
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
		pipeg.NewStage("validator", ValidateMessage, pipeg.StageConfig{
			Retry: pipeg.StageRetryDefault(),
		}),
	)

	message := &Message{}
	if err := pipe.Process(context.Background(), message); err != nil {
		panic(err)
	}

	logger.Info("pipeline completed", slog.String("data", string(message.Data)))
}
