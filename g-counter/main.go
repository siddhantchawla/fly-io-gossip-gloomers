package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	nodeId := n.ID()
	registered := false

	registerSelfIfNeeded := func() {
		if registered {
			return
		}

		ctx := context.Background()
		for {
			// Read current value + version
			val, _ := kv.Read(ctx, "participants")

			// extract participants from the value
			participants, ok := val.([]string)
			if !ok {
				participants = []string{}
			}

			// Check if self is already registered
			if slices.Contains(participants, nodeId) {
				registered = true
				return
			}

			newParticipants := append(participants, nodeId)

			// Try to atomically update with CAS
			err := kv.CompareAndSwap(ctx, "participants", participants, newParticipants, true)
			if err == nil {
				registered = true
				return
			}

			// Retry on CAS failure
			time.Sleep(100 * time.Millisecond)
		}
	}

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		total := 0
		val, _ := kv.Read(context.Background(), "participants")

		// extract participants from the value
		participants, ok := val.([]any)
		if ok {
			for _, id := range participants {
				// Add count of each participant
				if count, err := kv.ReadInt(context.Background(), id.(string)); err == nil {
					total += count
				}
			}
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": total,
		})
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		registerSelfIfNeeded()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		key := nodeId

		for {
			value, err := kv.ReadInt(context.Background(), key)
			if err != nil {
				value = 0
			}

			err = kv.Write(context.Background(), key, value+delta)
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
