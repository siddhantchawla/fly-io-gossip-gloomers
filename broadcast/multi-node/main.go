package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var nums []float64
	var neighbors []string
	n.Handle("broadcast", func (msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// extract the message from the body
		message := body["message"].(float64)
		
		for _, neighbor := range neighbors {
			// send the message to all neighbors except the sender
			if neighbor != msg.Src {
				// send the message to the neighbor
				n.Send(neighbor, body)
			}
		}
		nums = append(nums, message)

		
		body["type"] = "broadcast_ok"
		delete(body, "message")
	
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("read", func (msg maelstrom.Message) error {
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

	n.Handle("topology", func (msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
	
		// Update the message type.
		body["type"] = "topology_ok"
		// store the neighbors in the node
		neighbors = body["topology"].(map[string]any)[n.ID()].([]string)
		// remove the "topology" key from the body
		delete(body, "topology")
	
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
