package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Job func(workerID int, ctx *WorkerContext)

type queuedJob struct {
	run  Job
	done chan struct{}
}

type WorkerContext struct {
	ctx    context.Context
	cancel context.CancelFunc

	queue  chan queuedJob
	wg     sync.WaitGroup
	closed atomic.Bool
}

var ErrContextClosed = errors.New("worker context is closed")

func NewWorkerContext(parent context.Context, workerCount, queueSize int) *WorkerContext {
	if workerCount <= 0 {
		panic("workerCount must be > 0")
	}
	if queueSize <= 0 {
		queueSize = 1024
	}

	ctx, cancel := context.WithCancel(parent)

	wc := &WorkerContext{
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan queuedJob, queueSize),
	}

	for i := 1; i <= workerCount; i++ {
		workerID := i
		wc.wg.Add(1)
		go wc.workerLoop(workerID)
	}

	return wc
}

func (wc *WorkerContext) Post(job Job) error {
	if job == nil {
		return errors.New("job is nil")
	}
	if wc.closed.Load() {
		return ErrContextClosed
	}

	item := queuedJob{run: job}

	select {
	case <-wc.ctx.Done():
		return wc.ctx.Err()
	case wc.queue <- item:
		return nil
	}
}

func (wc *WorkerContext) Send(job Job) error {
	if job == nil {
		return errors.New("job is nil")
	}
	if wc.closed.Load() {
		return ErrContextClosed
	}

	done := make(chan struct{})
	item := queuedJob{run: job, done: done}

	select {
	case <-wc.ctx.Done():
		return wc.ctx.Err()
	case wc.queue <- item:
		select {
		case <-wc.ctx.Done():
			return wc.ctx.Err()
		case <-done:
			return nil
		}
	}
}

func (wc *WorkerContext) Stop() {
	if wc.closed.Swap(true) {
		return
	}
	wc.cancel()
	close(wc.queue)
	wc.wg.Wait()
}

func (wc *WorkerContext) workerLoop(workerID int) {
	defer wc.wg.Done()

	fmt.Printf("Started new worker: #%d\n", workerID)

	for {
		select {
		case <-wc.ctx.Done():
			return
		case item, ok := <-wc.queue:
			if !ok {
				return
			}
			item.run(workerID, wc)
			if item.done != nil {
				close(item.done)
			}
		}
	}
}

func waitEnter(prompt string) {
	fmt.Println(prompt)
	_, _ = bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func main() {
	runtime.GOMAXPROCS(4)

	wc := NewWorkerContext(context.Background(), 4, 4096)

	go func() {
		for i := 0; i < 20; i++ {
			_ = wc.Post(func(workerID int, ctx *WorkerContext) {
				fmt.Println(workerID)
				_ = ctx.Post(func(workerID2 int, _ *WorkerContext) {
					fmt.Println(workerID2)
				})
			})
		}
	}()

	waitEnter("Press Enter to enqueue a few more jobs...")

	for i := 0; i < 20; i++ {
		_ = wc.Post(func(workerID int, ctx *WorkerContext) {
			fmt.Println(workerID)
			_ = ctx.Post(func(workerID2 int, _ *WorkerContext) {
				fmt.Println(workerID2)
			})
		})
	}

	_ = wc.Send(func(workerID int, _ *WorkerContext) {
		fmt.Printf("Send executed on worker #%d\n", workerID)
	})

	waitEnter("Press Enter to stop...")

	wc.Stop()
	fmt.Println("Finished")

	time.Sleep(200 * time.Millisecond)
}
