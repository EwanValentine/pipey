package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/EwanValentine/pipey"
	"github.com/go-redis/redis/v8"
)

var (
	streamKey = "test"
	dataKey   = "data"
	EOF       = "eof"
	lastIDKey = "last_id"
)

func newStreamConnection() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

type streamer struct {
	client *redis.Client
}

func (s *streamer) setLastID(id string) {
	s.client.Set(context.Background(), lastIDKey, id, 0)
}

func (s *streamer) getLastID() string {
	res, err := s.client.Get(context.Background(), lastIDKey).Result()
	if err != nil {
		return "0"
	}
	return res
}

func (s *streamer) Listen(ctx context.Context, key string) chan []byte {
	out := make(chan []byte)
	go func() {
		for {
			res, err := s.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{key, s.getLastID()},
				Count:   1,
			}).Result()
			if err != nil {
				// Check if the parent context is cancelled
				select {
				case <-ctx.Done():
					log.Println("Context cancelled")
					close(out)
					return
				default:
					log.Println("Error reading stream:", err)
					continue
				}
			}

			for _, msg := range res[0].Messages {
				select {
				case <-ctx.Done():
					// We done
					close(out)
					return
				default:
					d := []byte(msg.Values[dataKey].(string))

					// If the client streamer sends an EOF, then we stop processing and return
					if string(d) == EOF {
						// Remove the EOF message, otherwise it will try to resume from the EOF
						// and just immediately exit again, hardly ideal, is it?
						s.client.XDel(ctx, key, msg.ID)

						// We done
						close(out)
						return
					}
					out <- d
				}
			}

			// Set last ID, so we can pick up where we left off, the next incoming message
			s.setLastID(res[0].Messages[0].ID)
		}
	}()
	return out
}

func (s *streamer) Write(ctx context.Context, key string, data []byte) error {
	err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]interface{}{
			"data": data,
		},
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

func processor(input []byte) []byte {
	return append(input, []byte(" processed")...)
}

func mapper(input []byte) []byte {
	return append(input, []byte(" mapped")...)
}

type Stages []func([]byte) []byte

func main() {
	client := newStreamConnection()

	s := &streamer{client: client}
	ctx := context.Background()

	inputStream := s.Listen(ctx, streamKey)

	// Send ten messages, followed by an EOF
	go func() {
		for i := 0; i < 10; i++ {
			data := fmt.Sprintf("Hello world %d", i)
			if err := s.Write(ctx, streamKey, []byte(data)); err != nil {
				log.Panic(err)
			}
		}

		// Hmmm 🤷🏻‍♂️
		time.Sleep(time.Second * 2)

		if err := s.Write(ctx, streamKey, []byte(EOF)); err != nil {
			log.Panic(err)
		}
	}()

	input := pipey.MergeIn(inputStream)

	output := pipey.Pipeline(input, Stages{processor})
	mapped := pipey.Map(output, mapper)
	filtered := pipey.Filter(mapped, func(input []byte) bool {
		if string(input) == "Hello world 2 processed mapped" {
			return false
		}
		return true
	})

	outA := make(chan []byte)
	outB := make(chan []byte)

	// Fan processed messages out to two different channels
	pipey.FanOut(filtered, outA, outB)

	// Iterate through the replicated output channels and write to the output streams
	go func() {
		for d := range outA {
			err := s.Write(ctx, "output:a", d)
			if err != nil {
				return
			}
		}
	}()

	for d := range outB {
		err := s.Write(ctx, "output:b", d)
		if err != nil {
			return
		}
	}
}
