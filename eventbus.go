package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Event represents a domain event
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// EventHandler is a function that handles an event
type EventHandler func(event Event) error

// EventBus manages asynchronous event distribution
type EventBus struct {
	handlers     map[string][]EventHandler
	handlersLock sync.RWMutex
	redis        *redis.Client
	writer       *kafka.Writer
	reader       *kafka.Reader
	deadLetterQ  *kafka.Writer
	maxRetries   int
}

// NewEventBus initializes the event bus with Kafka and Redis
func NewEventBus(redisClient *redis.Client) *EventBus {
	bus := &EventBus{
		handlers:    make(map[string][]EventHandler),
		redis:       redisClient,
		maxRetries:  3,
		deadLetterQ: kafka.NewWriter(kafka.WriterConfig{Brokers: []string{"localhost:29092"}, Topic: "dlq"}),
	}

	// Initialize Kafka writer (producer)
	bus.writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:29092"},
		Topic:        "events",
		RequiredAcks: kafka.RequireAll,
	})

	// Initialize Kafka reader (consumer)
	bus.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:29092"},
		Topic:   "events",
		GroupID: "event-bus-group",
	})

	return bus
}

// Subscribe registers a handler for a specific event type
func (eb *EventBus) Subscribe(eventType string, handler EventHandler) {
	eb.handlersLock.Lock()
	defer eb.handlersLock.Unlock()
	eb.handlers[eventType] = append(eb.handlers[eventType], handler)
}

// Publish publishes an event to Kafka topic
func (eb *EventBus) Publish(ctx context.Context, event Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: data,
	}

	if err := eb.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	log.Printf("Event published: %s (type: %s)", event.ID, event.Type)
	return nil
}

// StartConsumer starts consuming events from Kafka with retry logic and idempotency
func (eb *EventBus) StartConsumer(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := eb.reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		go eb.handleMessage(ctx, msg)
	}
}

// handleMessage processes a kafka message with retry and idempotency logic
func (eb *EventBus) handleMessage(ctx context.Context, msg kafka.Message) {
	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Unmarshal error: %v", err)
		return
	}

	// Check idempotency key in Redis
	idempotencyKey := fmt.Sprintf("event:%s:processed", event.ID)
	processed, err := eb.redis.Exists(ctx, idempotencyKey).Result()
	if err == nil && processed > 0 {
		log.Printf("Event already processed (idempotent): %s", event.ID)
		return
	}

	// Execute handlers with retry logic
	retryCount := 0
	for {
		handlers := eb.getHandlers(event.Type)
		if len(handlers) == 0 {
			log.Printf("No handlers for event type: %s", event.Type)
			break
		}

		success := true
		for _, handler := range handlers {
			if err := handler(event); err != nil {
				log.Printf("Handler error for event %s: %v", event.ID, err)
				success = false
			}
		}

		if success {
			// Mark as processed in Redis for idempotency
			eb.redis.SetEx(ctx, idempotencyKey, "1", 24*time.Hour)
			break
		}

		// Retry logic
		retryCount++
		if retryCount >= eb.maxRetries {
			log.Printf("Max retries exceeded for event %s, sending to DLQ", event.ID)
			eb.sendToDeadLetterQueue(ctx, event)
			break
		}

		wait := time.Duration(retryCount*retryCount) * time.Second
		log.Printf("Retrying event %s after %v (attempt %d/%d)", event.ID, wait, retryCount, eb.maxRetries)
		time.Sleep(wait)
	}
}

// sendToDeadLetterQueue sends failed events to DLQ
func (eb *EventBus) sendToDeadLetterQueue(ctx context.Context, event Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: data,
	}

	if err := eb.deadLetterQ.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to dlq: %w", err)
	}

	log.Printf("Event sent to DLQ: %s", event.ID)
	return nil
}

// getHandlers returns handlers for a specific event type
func (eb *EventBus) getHandlers(eventType string) []EventHandler {
	eb.handlersLock.RLock()
	defer eb.handlersLock.RUnlock()
	return eb.handlers[eventType]
}

// Close closes all connections
func (eb *EventBus) Close() error {
	if err := eb.writer.Close(); err != nil {
		return err
	}
	if err := eb.reader.Close(); err != nil {
		return err
	}
	if err := eb.deadLetterQ.Close(); err != nil {
		return err
	}
	return nil
}
