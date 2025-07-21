package pipeg

import "context"

type Retrier interface {
	Retry(ctx context.Context, do func() (any, error)) error
}
