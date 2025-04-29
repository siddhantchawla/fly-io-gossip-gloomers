package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TopicLog struct {
	Messages map[string][]float64
	Offsets  map[string]float64
}

func (t *TopicLog) Send(topic string, msg float64) float64 {
	t.Messages[topic] = append(t.Messages[topic], msg)
	return float64(len(t.Messages[topic]) - 1) // Return the offset
}

func (t *TopicLog) Poll(topic string, offset int) [][]float64 {
	var result [][]float64
	if msgs, ok := t.Messages[topic]; ok && offset < len(msgs) {
		for i := offset; i < offset+1; i++ {
			result = append(result, []float64{float64(i), msgs[i]})
		}
	}
	return result
}

func (t *TopicLog) GetOffset(topic string) float64 {
	return t.Offsets[topic]
}

func (t *TopicLog) Commit(topic string, offset float64) {
	t.Offsets[topic] = offset
}

func main() {
	n := maelstrom.NewNode()

	// Create a new TopicLog instance to store messages and offsets.
	kafkaLog := TopicLog{
		Messages: make(map[string][]float64),
		Offsets:  make(map[string]float64),
	}

	// Register handler for "send" message
	n.Handle("send", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		msgValue := body["msg"].(float64)

		// Send the message to the topic and get the offset.
		offset := kafkaLog.Send(key, msgValue)
		body["offset"] = offset
		body["type"] = "send_ok"
		delete(body, "key")
		delete(body, "msg")
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Register handler for "poll" message
	n.Handle("poll", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// read map of offsets from the body
		offsets := body["offsets"].(map[string]any)

		messages := make(map[string][][]float64)

		for k, v := range offsets {
			offset := int(v.(float64))
			// Poll the topic for messages starting from the given offset.
			msgs := kafkaLog.Poll(k, offset)
			messages[k] = msgs
		}

		body["type"] = "poll_ok"
		body["msgs"] = messages
		delete(body, "key")
		delete(body, "offsets")
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Register handler for "commit_offsets" message
	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// read map of offsets from the body
		offsets := body["offsets"].(map[string]any)

		for k, v := range offsets {
			offset := float64(v.(float64))
			// Commit the offset for the topic.
			kafkaLog.Commit(k, offset)
		}
		body["type"] = "commit_offsets_ok"
		delete(body, "key")
		delete(body, "offsets")
		return n.Reply(msg, body)
	})

	// Register handler for "list_committed_offsets" message
	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := body["keys"].([]any)

		committedOffsets := make(map[string]int)
		for _, k := range keys {
			// Get the committed offset for the topic.
			offset := kafkaLog.GetOffset(k.(string))
			committedOffsets[k.(string)] = int(offset)
		}

		body["type"] = "list_committed_offsets_ok"
		body["committed_offsets"] = committedOffsets
		delete(body, "keys")
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
