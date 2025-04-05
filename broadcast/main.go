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
	// var topology map[string]any
	n.Handle("broadcast", func (msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
	
		// Update the message type.
		body["type"] = "broadcast_ok"
		nums = append(nums, body["message"].(float64))
		// remove the "message" key from the body
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
		// store the "topology" in the node
		_ = body["topology"].(map[string]any)
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
