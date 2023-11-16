package pipey

import "sync"

type Stages[T any] []func(T) T

// MergeIn takes a slice of channels and returns a channel of the combined input values.
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

// FanOut takes a channel and replicates the input values into multiple channels.
func FanOut[T any](in <-chan T, outs ...chan T) {
	distChans := make([]chan T, len(outs))
	for i := range outs {
		distChans[i] = make(chan T)
		go func(out chan T, distChan chan T) {
			for v := range distChan {
				out <- v
			}
			close(out)
		}(outs[i], distChans[i])
	}

	go func() {
		for v := range in {
			for _, distChan := range distChans {
				distChan <- v
			}
		}
		for _, distChan := range distChans {
			close(distChan)
		}
	}()
}

// Pipeline takes series of stages and returns a channel that's the output of the last stage.
func Pipeline[T any](input <-chan T, processors []func(T) T) <-chan T {
	current := input
	for _, processor := range processors {
		current = process(current, processor)
	}
	return current
}

func process[T any](input <-chan T, processor func(T) T) <-chan T {
	output := make(chan T)
	go func() {
		defer close(output)
		for i := range input {
			output <- processor(i)
		}
	}()
	return output
}

// Filter takes a channel and returns a channel of values that pass the given condition.
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

// Map takes a channel and returns a channel of values that have been transformed by the given function.
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

// Catch takes a channel and returns a channel of values that have been transformed by the given function,
// along with any errors that occurred.
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
