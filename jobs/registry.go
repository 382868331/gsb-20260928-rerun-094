package jobs

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Registry struct {
	mu   sync.RWMutex
	defs map[string]Definition
}

func NewRegistry(defs ...Definition) *Registry {
	r := &Registry{defs: map[string]Definition{}}
	for _, def := range defs {
		r.Register(def)
	}
	return r
}

func (r *Registry) Register(def Definition) {
	if def.Execute == nil || def.Type == "" {
		panic("invalid job definition")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defs[def.Type] = def
}

func (r *Registry) Get(typ string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.defs[typ]
	return def, ok
}

func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	types := make([]string, 0, len(r.defs))
	for typ := range r.defs {
		types = append(types, typ)
	}
	sort.Strings(types)
	return types
}

func BuiltinRegistry() *Registry {
	return NewRegistry(
		Definition{
			Type:        "sleep",
			SafeToRetry: true,
			Execute: func(ctx context.Context, in Input) (string, error) {
				d, err := strconv.Atoi(in.Payload)
				if err != nil || d < 0 {
					d = 100
				}
				select {
				case <-time.After(time.Duration(d) * time.Millisecond):
					return "slept", nil
				case <-ctx.Done():
					return "", ctx.Err()
				}
			},
		},
		Definition{
			Type:        "echo",
			SafeToRetry: true,
			Execute: func(_ context.Context, in Input) (string, error) {
				return "echo: " + in.Payload, nil
			},
		},
		Definition{
			Type:        "flaky",
			SafeToRetry: true,
			Execute: func(_ context.Context, in Input) (string, error) {
				if in.Attempt == 1 {
					return "", fmt.Errorf("temporary failure on first attempt")
				}
				return "ok after retry: " + in.Payload, nil
			},
		},
		Definition{
			Type:        "unsafe_payment",
			SafeToRetry: false,
			Execute: func(_ context.Context, in Input) (string, error) {
				return "charged: " + in.Payload, nil
			},
		},
		Definition{
			Type:        "unsafe_flaky",
			SafeToRetry: false,
			Execute: func(_ context.Context, in Input) (string, error) {
				return "", fmt.Errorf("downstream status is unknown on attempt %d", in.Attempt)
			},
		},
	)
}
