package pipeg

import (
	"errors"
	"fmt"

	"github.com/cenkalti/backoff/v5"
)

var (
	ErrStageTimeout = errors.New("stage has timed out")
)

type ReasonedError struct {
	Err    error
	Reason string
}

var _ error = (*ReasonedError)(nil)

// Error implements error.
func (r *ReasonedError) Error() string {
	return fmt.Sprintf("%s: %s", r.Reason, r.Err)
}

func (r *ReasonedError) Unwrap() error {
	return r.Err
}

func Reason(err error, reason string) error {
	return &ReasonedError{Err: err, Reason: reason}
}

func IsReason(err error) (*ReasonedError, bool) {
	r, ok := err.(*ReasonedError)

	return r, ok
}

type BreakError struct {
	ReasonedError
}

var _ error = (*BreakError)(nil)

func Break(err error, reason string) error {
	return &BreakError{ReasonedError: ReasonedError{Err: err, Reason: reason}}
}

func IsBreak(err error) (*BreakError, bool) {
	b, ok := err.(*BreakError)

	return b, ok
}

func PermanentError(err error) error {
	return backoff.Permanent(err)
}
