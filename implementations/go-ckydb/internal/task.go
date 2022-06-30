package internal

import (
	"time"
)

type Worker interface {
	Start() error
	Stop() error
	IsRunning() bool
}

type Task struct {
	done      chan bool
	interval  time.Duration
	work      func()
	isRunning bool
}

func NewTask(interval time.Duration, work func()) *Task {
	return &Task{
		done:      make(chan bool),
		interval:  interval,
		work:      work,
		isRunning: false,
	}
}

// Start starts the task that runs the work in a go routine
func (t *Task) Start() error {
	if t.isRunning {
		return ErrAlreadyRunning
	}

	go func(ch chan bool, work func()) {
		tick := time.NewTicker(t.interval)
		defer tick.Stop()

		for {
			select {
			case <-ch:
				// respond back that it is done
				ch <- true
				return
			case <-tick.C:
				work()
			}
		}
	}(t.done, t.work)

	t.isRunning = true

	return nil
}

// Stop Sends an instruction to the task to stop running
func (t *Task) Stop() error {
	if !t.isRunning {
		return ErrNotRunning
	}

	t.done <- true

	// wait for the task to respond back
	<-t.done
	t.isRunning = false
	return nil
}

// IsRunning returns true if the task is still running
func (t *Task) IsRunning() bool {
	return t.isRunning
}
