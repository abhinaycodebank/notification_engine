package main

import (
	"fmt"

	"notification-engine/internal/database"
	"notification-engine/internal/kafka"
	"notification-engine/internal/models"
)

func main() {
	fmt.Println("Matching Engine worker pool successfully initialized and polling Kafka partitions...")

	// Infinite consumer loop processing streaming messages
	for event := range kafka.BrokerInstance.RawEvents {
		fmt.Printf("[MATCHING ENGINE]: Processing Event ID: %s (%s)\n", event.ID, event.EventType)

		// 1. Fetch subscriptions from the system registry store
		// (In a future step, this will check Redis cache first)
		subscriptions := database.Instance.GetAll()

		// 2. Evaluate rules against active subscriptions
		for _, sub := range subscriptions {
			if isMatch(event, sub) {
				fmt.Printf("  └─► [MATCH FOUND]: Event %s satisfies criteria for subscriber '%s'\n", event.ID, sub.Name)

				// Create the delivery task payload tracking attempt count
				task := models.DeliveryTask{
					Event:        event,
					Subscription: sub,
					Attempt:      1, // Initial dispatch attempt
				}

				// 3. Forward task to the Kafka delivery pipeline topic
				_ = kafka.BrokerInstance.PublishDeliveryTask(task)
			}
		}
	}
}

// Simple filter check: every key-value pair in sub.Filters must match event.Data or core fields
func isMatch(event models.Event, sub models.Subscription) bool {
	// If the subscription has no filters defined, treat it as a wildcard catch-all
	if len(sub.Filters) == 0 {
		return true
	}

	for filterKey, filterVal := range sub.Filters {
		// Special structural field evaluation fallback checks
		if filterKey == "event_type" {
			if event.EventType != filterVal {
				return false
			}
			continue
		}
		if filterKey == "source" {
			if event.Source != filterVal {
				return false
			}
			continue
		}

		// Payload body payload exact string value evaluation matches
		eventVal, exists := event.Data[filterKey]
		if !exists || eventVal != filterVal {
			return false // Criterion missing or mismatched
		}
	}

	return true // All criteria matched perfectly
}
