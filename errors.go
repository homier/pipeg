package pipeg

import "fmt"

type BreakError struct {
	Err    error
	Reason string
}

var _ error = (*BreakError)(nil)

// Error implements error.
func (b *BreakError) Error() string {
	return fmt.Sprintf("%s: %w", b.Reason, b.Err.Error())
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
