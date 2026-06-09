package models

import "time"

// DeliveryAudit represents the execution log of a single webhook dispatch attempt.
type DeliveryAudit struct {
	ID             string    `json:"id"`
	SubscriptionID string    `json:"subscription_id"`
	EventID        string    `json:"event_id"`
	Attempt        int       `json:"attempt"`
	HTTPStatus     int       `json:"http_status"`
	ResponseBody   string    `json:"response_body"`
	DeliveryStatus string    `json:"delivery_status"` // "delivered", "failed"
	CreatedAt      time.Time `json:"created_at"`
}
