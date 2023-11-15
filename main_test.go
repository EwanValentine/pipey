package piper

import (
	"errors"
	"testing"
)

func TestMergeIn(t *testing.T) {
	a := make(chan int)
	b := make(chan int)
	c := make(chan int)

	go func() {
		a <- 1
		a <- 2
		a <- 3
	}()

	output := MergeIn(a, b, c)

	for i := 1; i <= 3; i++ {
		if <-output != i {
			t.Errorf("Expected %d, got %d", i, <-output)
		}
	}
}

func double(n int) int {
	return n * 2
}

func TestFanOut(t *testing.T) {
	in := make(chan int)
	out1 := make(chan int)
	out2 := make(chan int)

	FanOut(in, out1, out2)

	for i := 1; i <= 3; i++ {
		in <- i
		if <-out1 != i {
			t.Errorf("Expected %d, got %d", i, <-out1)
		}
		if <-out2 != i {
			t.Errorf("Expected %d, got %d", i, <-out2)
		}
	}
}

func TestPipeline(t *testing.T) {
	input1 := make(chan int)
	go func() {
		input1 <- 1
		close(input1)
	}()

	input2 := make(chan int)
	go func() {
		input2 <- 2
		close(input2)
	}()

	stage := Pipeline(MergeIn(input1, input2), double)

	out1 := <-stage
	if out1 != 2 {
		t.Errorf("Expected 2, got %d", out1)
	}

	out2 := <-stage
	if out2 != 4 {
		t.Errorf("Expected 4, got %d", out2)
	}
}

func TestFilter(t *testing.T) {
	input := make(chan int)
	go func() {
		input <- 1
		input <- 2
		input <- 3
		close(input)
	}()

	stage := Filter(input, func(n int) bool {
		return n != 2
	})

	out1 := <-stage
	if out1 != 1 {
		t.Errorf("Expected 1, got %d", out1)
	}

	out2 := <-stage
	if out2 != 3 {
		t.Errorf("Expected 3, got %d", out2)
	}
}

func TestMap(t *testing.T) {
	input := make(chan int)
	go func() {
		input <- 1
		input <- 2
		input <- 3
		close(input)
	}()

	stage := Map(input, double)

	out1 := <-stage
	if out1 != 2 {
		t.Errorf("Expected 2, got %d", out1)
	}

	out2 := <-stage
	if out2 != 4 {
		t.Errorf("Expected 4, got %d", out2)
	}
}

func TestFanOutFanIn(t *testing.T) {
	input := make(chan int)
	go func() {
		input <- 1
		input <- 2
		input <- 3
		close(input)
	}()

	stage := FanOutFanIn(input, double, 2)

	out1 := <-stage
	if out1 != 2 {
		t.Errorf("Expected 2, got %d", out1)
	}

	out2 := <-stage
	if out2 != 4 {
		t.Errorf("Expected 4, got %d", out2)
	}
}

func TestBatch(t *testing.T) {
	input := make(chan int)
	go func() {
		input <- 1
		input <- 2
		input <- 3
		close(input)
	}()

	stage := Batch(input, 2)

	out1 := <-stage
	if len(out1) != 2 {
		t.Errorf("Expected 2, got %d", len(out1))
	}

	out2 := <-stage
	if len(out2) != 1 {
		t.Errorf("Expected 1, got %d", len(out2))
	}
}

func TestCatch(t *testing.T) {
	input := make(chan int)
	go func() {
		input <- 1
		input <- 2
		input <- 3
		close(input)
	}()

	stage := Catch(input, func(n int) (int, error) {
		return 1, errors.New("test")
	})

	out1 := <-stage
	if out1.Err.Error() != "test" {
		t.Errorf("Expected err, got %d", out1.Err)
	}

	if out1.Value != 1 {
		t.Errorf("Expected 1, got %d", out1.Value)
	}
}
