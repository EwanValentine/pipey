package piper

import "sync"

func MergeIn[T any](streams ...<-chan T) chan T {
	out := make(chan T)
	var wg sync.WaitGroup

	output := func(stream <-chan T) {
		defer wg.Done()
		for val := range stream {
			out <- val
		}
	}

	wg.Add(len(streams))
	for _, stream := range streams {
		go output(stream)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func FanOut[T any](in chan T, outs ...chan T) {
	go func() {
		for {
			v := <-in
			for _, out := range outs {
				out <- v
			}
		}
	}()
}

func Pipeline[T any](input <-chan T, processor func(T) T) <-chan T {
	output := make(chan T)
	go func() {
		defer close(output)
		for i := range input {
			output <- processor(i)
		}
	}()
	return output
}

func Filter[T any](input <-chan T, condition func(T) bool) <-chan T {
	output := make(chan T)
	go func() {
		defer close(output)
		for item := range input {
			if condition(item) {
				output <- item
			}
		}
	}()
	return output
}

func Map[T any, U any](input <-chan T, transform func(T) U) <-chan U {
	output := make(chan U)
	go func() {
		defer close(output)
		for item := range input {
			output <- transform(item)
		}
	}()
	return output
}

// FanOutFanIn
func FanOutFanIn[T any](input <-chan T, worker func(T) T, numWorkers int) <-chan T {
	output := make(chan T)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for item := range input {
				output <- worker(item)
			}
		}()
	}
	return output
}

// Batch takes a channel and streams the results into batches of the given size.
func Batch[T any](input <-chan T, batchSize int) <-chan []T {
	output := make(chan []T)
	go func() {
		defer close(output)
		for {
			batch := make([]T, 0, batchSize)
			for item := range input {
				batch = append(batch, item)
				if len(batch) == batchSize {
					break
				}
			}
			if len(batch) == 0 {
				return
			}
			output <- batch
		}
	}()
	return output
}

type Result[T any] struct {
	Value T
	Err   error
}

// Catch is a pipeline stage that allows you to handle errors in a pipeline.
// It takes a channel of values and a function that takes a value and returns
// a value and an error. It returns a channel of Result[T] where Result[T] is
// a struct containing a value and an error. If the function returns an error,
// the error will be sent to the Result[T].Err field. If the function returns
// a value, the value will be sent to the Result[T].Value field.
// Diagram:
func Catch[T any](input <-chan T, processor func(T) (T, error)) <-chan Result[T] {
	output := make(chan Result[T])
	go func() {
		defer close(output)
		for item := range input {
			result, err := processor(item)
			output <- Result[T]{Value: result, Err: err}
		}
	}()
	return output
}
