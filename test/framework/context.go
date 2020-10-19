// +build e2e_redis_op

package framework

import (
	"fmt"
	"testing"

	"golang.org/x/sync/errgroup"
)

type TestCtx struct {
	ID         string
	cleanUpFns []finalizerFn
}

type finalizerFn func() error

func NewTestCtx(t *testing.T, name string) TestCtx {
	return TestCtx{
		ID: name,
	}
}

func (ctx *TestCtx) Cleanup(t *testing.T) {
	var eg errgroup.Group
	fmt.Println("\nRunning cleanup")

	for i := len(ctx.cleanUpFns) - 1; i >= 0; i-- {
		eg.Go(ctx.cleanUpFns[i])
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func (ctx *TestCtx) AddFinalizerFn(fn finalizerFn) {
	ctx.cleanUpFns = append(ctx.cleanUpFns, fn)
}

func (ctx *TestCtx) PopFinalizer() {
	if len(ctx.cleanUpFns) > 0 {
		ctx.cleanUpFns = ctx.cleanUpFns[:len(ctx.cleanUpFns)-1]
	}
}
