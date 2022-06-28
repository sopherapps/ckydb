package internal

import (
	"time"
)

type Worker interface {
	Start() error
	Stop() error
}

type Task struct {
	c         chan struct{}
	interval  time.Duration
	work      func()
	isRunning bool
}

func NewTask(interval time.Duration, work func()) *Task {
	return &Task{
		c:         make(chan struct{}),
		interval:  interval,
		work:      work,
		isRunning: false,
	}
}

func (t *Task) Start() error {
	if t.isRunning {
		return ErrAlreadyRunning
	}

	go func(ch chan struct{}, work func()) {
		tick := time.NewTicker(t.interval)
		defer tick.Stop()

		for {
			select {
			case <-ch:
				ch <- struct{}{}
				return
			case <-tick.C:
				work()
			}
		}
	}(t.c, t.work)

	t.isRunning = true

	return nil
}

func (t *Task) Stop() error {
	if !t.isRunning {
		return ErrNotRunning
	}

	t.c <- struct{}{}
	<-t.c
	t.isRunning = false
	return nil
}
