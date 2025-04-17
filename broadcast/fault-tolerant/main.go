package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var nums []float64
	var neighbors []string
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// extract the message from the body
		message := body["message"].(float64)

		// if the message is not already in the nums slice, add it to the slice and send it to all neighbors
		if !slices.Contains(nums, message) {

			for _, neighbor := range neighbors {
				// send the message to all neighbors except the sender
				if neighbor != msg.Src {
					// send the message to the neighbor
					n.Send(neighbor, body)
				}
			}
			nums = append(nums, message)
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type.
		body["type"] = "read_ok"
		// add the "messages" key to the body
		body["messages"] = nums

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type.
		body["type"] = "topology_ok"
		// store the neighbors in the node
		topology := body["topology"].(map[string]any)
		rawNeighbors := topology[n.ID()].([]any)

		neighbors = make([]string, 0, len(rawNeighbors))
		for _, val := range rawNeighbors {
			neighbors = append(neighbors, val.(string))
		}
		// remove the "topology" key from the body
		delete(body, "topology")

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	// Start a goroutine to send broadcast messages to neighbors every 2 seconds
	go func() {
		for {
			// Wait for 2 seconds before sending the broadcast message
			time.Sleep(2 * time.Second)
			for _, message := range nums {
				for _, neighbor := range neighbors {
					n.Send(neighbor, map[string]any{
						"type":    "broadcast",
						"message": message,
					})
				}
			}
		}
	}()

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
