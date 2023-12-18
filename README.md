# Pipey

A generic Pipeline framework for data processing in the Go programming language.

## Examples

### Fan In

Fan in takes multiple input channels and combines the values to a single output channel.

```golang
output := FanIn(channelA, channelB, channelC)
```

### Fan Out

Fan out replicates a single input channel into one or more output channels.

```golang
inA := make(chan int)

outputA := make(chan int)
outputB := make(chan int)

done := FanOut(inA, outputA, outputB)
<-done
```

### Filter

Filter filters values on a given channel and returns the filtered results.

```golang
input := make(chan int)
output := Filter(input, func(n int) bool {
  return n != 2
})
```

### Map

Map iterates over an input channel, modifies each value and maps it back to an output stream.

```golang
input := make(chan int)
output := Map(input, func(i int) int {
  return i * 2
})
```

### Fan Out Fan In

Spawns a defined number of go routines to perform a task, the results of which are then combined back into a single channel.

```golang
input := make(chan int)
output := FanOutFanIn(input, func(i int) int {
  return i * 2
}, 2)
```
