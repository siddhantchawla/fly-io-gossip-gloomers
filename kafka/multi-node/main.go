package main

import (
	"context"
	"encoding/json"
	"log"

	"maps"
	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	Topic  = "messages"
	Offset = "offsets"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	// Utility to deserialize stored messages
	readMessages := func() map[string][]float64 {
		result := make(map[string][]float64)
		data, err := kv.Read(context.Background(), Topic)
		if err != nil {
			return result
		}
		if m, ok := data.(map[string]any); ok {
			for topic, raw := range m {
				if arr, ok := raw.([]any); ok {
					for _, item := range arr {
						result[topic] = append(result[topic], item.(float64))
					}
				}
			}
		}
		return result
	}

	// Utility to read committed offsets
	readOffsets := func() map[string]float64 {
		result := make(map[string]float64)
		data, err := kv.Read(context.Background(), Offset)
		if err != nil {
			return result
		}
		if m, ok := data.(map[string]any); ok {
			for topic, val := range m {
				result[topic] = val.(float64)
			}
		}
		return result
	}

	// SEND
	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topic := body["key"].(string)
		message := body["msg"].(float64)

		var offset int
		for {
			oldMessages := readMessages()

			// Make a deep copy
			newMessages := make(map[string][]float64)
			for k, v := range oldMessages {
				newMessages[k] = slices.Clone(v)
			}

			newMessages[topic] = append(newMessages[topic], message)
			offset = len(newMessages[topic]) - 1

			err := kv.CompareAndSwap(context.Background(), Topic, oldMessages, newMessages, true)
			if err == nil {
				break
			}
		}

		body["offset"] = float64(offset)
		body["type"] = "send_ok"
		delete(body, "key")
		delete(body, "msg")
		return n.Reply(msg, body)
	})

	// POLL
	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := body["offsets"].(map[string]any)

		messages := readMessages()
		replyMsgs := make(map[string][][]float64)

		for topic, startOffset := range offsets {
			msgs := messages[topic]
			start := int(startOffset.(float64))
			if start < len(msgs) {
				replyMsgs[topic] = append(replyMsgs[topic], []float64{float64(start), msgs[start]})
			}
		}

		body["type"] = "poll_ok"
		body["msgs"] = replyMsgs
		delete(body, "offsets")
		return n.Reply(msg, body)
	})

	// COMMIT OFFSETS
	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]any)

		for {
			oldOffsets := readOffsets()

			newOffsets := make(map[string]float64)
			maps.Copy(newOffsets, oldOffsets)
			for k, v := range offsets {
				newOffsets[k] = v.(float64)
			}

			err := kv.CompareAndSwap(context.Background(), Offset, oldOffsets, newOffsets, true)
			if err == nil {
				break
			}
		}

		body["type"] = "commit_offsets_ok"
		delete(body, "offsets")
		return n.Reply(msg, body)
	})

	// LIST COMMITTED OFFSETS
	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)

		allOffsets := readOffsets()
		replyOffsets := make(map[string]float64)
		for _, key := range keys {
			if val, ok := allOffsets[key.(string)]; ok {
				replyOffsets[key.(string)] = val
			}
		}

		body["type"] = "list_committed_offsets_ok"
		body["offsets"] = replyOffsets
		delete(body, "keys")
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
