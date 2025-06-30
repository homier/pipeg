package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/homier/pipeg"
)

type Message struct {
	Counter int `json:"counter"`
}

func IncrementCounter(ctx context.Context, _ *slog.Logger, message *Message) error {
	message.Counter++

	return nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	pipe := pipeg.New(
		"simple", pipeg.Config{Logger: logger, Verbose: true},
		pipeg.NewStage("incrementor", IncrementCounter, pipeg.StageConfig{}),
	)

	message := &Message{}
	if err := pipe.Process(context.Background(), message); err != nil {
		panic(err)
	}

	logger.Info("message has been processed", slog.Int("counter", message.Counter))
}
