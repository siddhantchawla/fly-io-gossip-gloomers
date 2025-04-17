package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func sendWithRetry(n *maelstrom.Node, to string, msg map[string]any) {
	for retry := 1; retry <= 100; retry++ {
		err := n.Send(to, msg)
		if err == nil {
			break // success!
		}
		time.Sleep(time.Duration(retry) * 100 * time.Millisecond)
	}
}

func main() {
	n := maelstrom.NewNode()

	var (
		mu        sync.Mutex
		messages  = make(map[float64]bool) // store seen messages
		neighbors []string
	)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(float64)

		mu.Lock()
		_, seen := messages[message]
		if !seen {
			messages[message] = true
			mu.Unlock()

			for _, neighbor := range neighbors {
				if neighbor != msg.Src {
					go sendWithRetry(n, neighbor, map[string]any{
						"type":    "broadcast",
						"message": message,
					})
				}
			}
		} else {
			mu.Unlock()
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		result := make([]float64, 0, len(messages))
		for m := range messages {
			result = append(result, m)
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": result,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology := body["topology"].(map[string]any)
		rawNeighbors := topology[n.ID()].([]any)

		neighbors = make([]string, 0, len(rawNeighbors))
		for _, val := range rawNeighbors {
			neighbors = append(neighbors, val.(string))
		}

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
