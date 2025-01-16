package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/homier/pipeg"
)

type Message struct {
	Data []byte `json:"data"`
}

type Validator struct {
	pipeg.Stage[*Message]
}

func NewValidator() *Validator {
	return &Validator{Stage: pipeg.Stage[*Message]{Cfg: pipeg.StageConfig{
		Name:  "validator",
		Retry: pipeg.StageRetryDefault(),
	}}}
}

func (v *Validator) Process(ctx context.Context, message *Message) error {
	if message.Data == nil {
		message.Data = []byte("data")

		return errors.New("data must not be empty")
	}

	return nil
}

func main() {
	pipe := pipeg.New("retries", pipeg.Config{}, NewValidator())
	message := &Message{}

	if err := pipe.Process(context.Background(), message); err != nil {
		panic(err)
	}

	fmt.Printf("pipe succeeded: %s\n", string(message.Data))
}
