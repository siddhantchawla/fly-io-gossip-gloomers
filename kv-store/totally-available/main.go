package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	kvstore := make(map[int]int)

	n.Handle("txn", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Extract the transaction details from the message body. txn is a list of lists
		// where each inner list contains a key and a value.
		txn, _ := body["txn"].([]any)
		for _, op := range txn {
			// Each operation is a list of [operation, key, value].
			operation, _ := op.([]any)
			if operation[0] == "w" {
				// Write operation
				key, _ := operation[1].(float64)
				value, _ := operation[2].(float64)
				kvstore[int(key)] = int(value)
			}
			if operation[0] == "r" {
				// Read operation
				key, _ := operation[1].(float64)
				// Check if the key exists in the kvstore
				if value, ok := kvstore[int(key)]; ok {
					// If it exists, set the value in the body
					operation[2] = float64(value)
				}
			}
		}

		body["type"] = "txn_ok"
		body["txn"] = txn
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
