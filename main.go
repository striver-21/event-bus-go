package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Redis client for idempotency and state management
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Println("Redis unavailable, continuing in offline mode for demo")
	}

	// Initialize event bus
	bus := NewEventBus(redisClient)

	// Subscribe to events
	bus.Subscribe("user.created", handleUserCreated)
	bus.Subscribe("order.placed", handleOrderPlaced)
	bus.Subscribe("payment.processed", handlePaymentProcessed)

	// Start consumer
	consumerDone := make(chan struct{})
	go func() {
		if err := bus.StartConsumer(ctx); err != nil {
			log.Printf("Consumer error: %v\n", err)
		}
		close(consumerDone)
	}()

	// Publish sample events
	go publishSampleEvents(bus, ctx)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\nShutting down...")
	bus.Close()
	<-consumerDone
}

func publishSampleEvents(bus *EventBus, ctx context.Context) {
	time.Sleep(2 * time.Second) // Wait for consumer to start

	events := []Event{
		{
			ID:        "evt_001",
			Type:      "user.created",
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"user_id": "usr_123",
				"email":   "user@example.com",
			},
		},
		{
			ID:        "evt_002",
			Type:      "order.placed",
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"order_id": "ord_456",
				"amount":   99.99,
			},
		},
		{
			ID:        "evt_003",
			Type:      "payment.processed",
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"payment_id": "pay_789",
				"status":     "success",
			},
		},
	}

	for _, event := range events {
		if err := bus.Publish(ctx, event); err != nil {
			log.Printf("Publish error: %v\n", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func handleUserCreated(event Event) error {
	fmt.Printf("✓ User created event handled: %+v\n", event.Data)
	return nil
}

func handleOrderPlaced(event Event) error {
	fmt.Printf("✓ Order placed event handled: %+v\n", event.Data)
	return nil
}

func handlePaymentProcessed(event Event) error {
	fmt.Printf("✓ Payment processed event handled: %+v\n", event.Data)
	return nil
}
