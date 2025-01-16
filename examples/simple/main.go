package main

import (
	"context"
	"fmt"

	"github.com/homier/pipeg"
)

type Message struct {
	Counter int `json:"counter"`
}

type Incrementor struct {
	pipeg.Stage[*Message]
}

func NewIncrementor() *Incrementor {
	return &Incrementor{Stage: pipeg.Stage[*Message]{
		Cfg: pipeg.StageConfig{
			Name: "incrementor",
		},
	}}
}

func (i *Incrementor) Process(ctx context.Context, message *Message) error {
	message.Counter++

	return nil
}

// Config implements pipeg.Stage.
var _ pipeg.Stager[*Message] = (*Incrementor)(nil)

func main() {
	pipe := pipeg.New("simple", pipeg.Config{}, NewIncrementor())

	message := &Message{}
	if err := pipe.Process(context.Background(), message); err != nil {
		panic(err)
	}

	fmt.Printf("Message has been processed, final counter: %d\n", message.Counter)
}
