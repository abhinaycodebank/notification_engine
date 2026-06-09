package models

import "time"

// Subscription represents a registered webhook target consumer.
type Subscription struct {
	ID         string            `json:"id"`          // Unique identifier (UUID string)
	Name       string            `json:"name"`        // Friendly identifier name
	WebhookURL string            `json:"webhook_url"` // Target destination URL
	Filters    map[string]string `json:"filters"`     // Simple key-value exact match filters
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// DeliveryTask wraps an Event and the matching destination Subscription.
// This object is what travels over the internal Kafka delivery queues.
type DeliveryTask struct {
	Event        Event        `json:"event"`
	Subscription Subscription `json:"subscription"`
	Attempt      int          `json:"attempt"` // Tracks retry counts across workers
}
