package models

import "time"

// Event represents an incoming system event payload.
type Event struct {
	ID        string            `json:"id"`         // Unique identifier for the event
	EventType string            `json:"event_type"` // e.g., "user.signup", "order.placed"
	Source    string            `json:"source"`     // Originating service name
	Data      map[string]string `json:"data"`       // Simple key-value data payload for filtering
	CreatedAt time.Time         `json:"created_at"` // Timestamp of when the event occurred
}
