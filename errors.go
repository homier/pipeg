package pipeg

import (
	"errors"
	"fmt"

	"github.com/cenkalti/backoff/v5"
)

var (
	ErrStageTimeout = errors.New("stage has timed out")
)

type BreakError struct {
	Err    error
	Reason string
}

var _ error = (*BreakError)(nil)

// Error implements error.
func (b *BreakError) Error() string {
	return fmt.Sprintf("%s: %s", b.Reason, b.Err)
}

func (b *BreakError) Unwrap() error {
	return b.Err
}

func Break(err error, reason string) error {
	return &BreakError{Err: err, Reason: reason}
}

func IsBreak(err error) (*BreakError, bool) {
	b, ok := err.(*BreakError)

	return b, ok
}

func PermanentError(err error) error {
	return backoff.Permanent(err)
}
