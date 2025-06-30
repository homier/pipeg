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

func (m *Message) Inc() {
	m.Counter += 1
}

func (m *Message) Dec() {
	m.Counter -= 1
}

func (m *Message) Get() int {
	return m.Counter
}

type Counter interface {
	Inc()
	Dec()
	Get() int
}

func IncrementCounter(ctx context.Context, _ *slog.Logger, message Counter) error {
	message.Inc()

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

	logger.Info("message has been processed", slog.Int("counter", message.Get()))
}
