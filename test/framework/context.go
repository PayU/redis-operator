// +build e2e_redis_op

package framework

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"golang.org/x/sync/errgroup"
)

type TestCtx struct {
	ID         string
	cleanUpFns []finalizerFn
	testHandle *testing.T
}

type finalizerFn func() error

func NewTestCtx(t *testing.T, name string) TestCtx {
	ctx := TestCtx{
		ID:         name,
		testHandle: t,
	}

	ctx.SetupSignalHandler()

	return ctx
}

func (ctx *TestCtx) Cleanup() {
	var eg errgroup.Group
	fmt.Println("[E2E] Cleanup")

	for i := len(ctx.cleanUpFns) - 1; i >= 0; i-- {
		eg.Go(ctx.cleanUpFns[i])
	}

	if err := eg.Wait(); err != nil {
		ctx.testHandle.Fatal(err)
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

func (ctx *TestCtx) SetupSignalHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		ctx.Cleanup()
		ctx.testHandle.Fatal("Test interrupted by SIGTERM")
	}()
}
