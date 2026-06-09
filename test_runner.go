package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"notification-engine/internal/database"
	"notification-engine/internal/kafka"
	"notification-engine/internal/models"

	"github.com/gin-gonic/gin"
)

func main() {
	// 1. Pre-seed our DB Store with an active Subscription rule
	// Tip: You can replace this URL string with a real webhook.site link to test live tracking!
	mockSub := models.Subscription{
		ID:         "sub-prod-001",
		Name:       "Production Dispatcher Hub",
		WebhookURL: "https://httpbin.org",
		Filters: map[string]string{
			"event_type": "payment.success",
		},
	}
	_ = database.Instance.Create(&mockSub)
	fmt.Printf("[SETUP]: Configuration loaded for rule subscriber: '%s'\nTarget URL: %s\n", mockSub.Name, mockSub.WebhookURL)

	// 2. Start Matching Engine background worker routine
	go func() {
		for event := range kafka.BrokerInstance.RawEvents {
			for _, sub := range database.Instance.GetAll() {
				if event.EventType == sub.Filters["event_type"] {
					fmt.Printf("\n[ENGINE]: Match identified on event '%s' for rule '%s'\n", event.ID, sub.Name)
					_ = kafka.BrokerInstance.PublishDeliveryTask(models.DeliveryTask{
						Event: event, Subscription: sub, Attempt: 1,
					})
				}
			}
		}
	}()

	// 3. Start Delivery Worker Pool routine (2 concurrent workers)
	client := &http.Client{Timeout: 4 * time.Second}
	for w := 1; w <= 2; w++ {
		go func(workerID int) {
			fmt.Printf("[WORKER %d]: Listening for matched dispatch tasks...\n", workerID)
			for task := range kafka.BrokerInstance.DeliveryTasks {
				fmt.Printf("[WORKER %d]: Handling task Event %s -> Sub %s (Attempt %d)\n", workerID, task.Event.ID, task.Subscription.Name, task.Attempt)

				jsonData, _ := json.Marshal(task.Event)
				resp, err := client.Do(&http.Request{
					Method: "POST",
					URL:    mustParseURL(task.Subscription.WebhookURL),
					Header: http.Header{"Content-Type": []string{"application/json"}},
					Body:   io.NopCloser(bytes.NewBuffer(jsonData)),
				})

				if err != nil {
					fmt.Printf("[WORKER %d ERROR]: Connection broke: %v\n", workerID, err)
					if task.Attempt < 3 {
						task.Attempt++
						_ = kafka.BrokerInstance.PublishDeliveryTask(task) // At-least-once retry loop back into queue
					}
					continue
				}

				fmt.Printf("[WORKER %d RESPONSE]: Received HTTP %d from endpoint!\n", workerID, resp.StatusCode)
				fmt.Printf("[AUDIT STAMP]: Logged execution result for event %s to Audit Table.\n", task.Event.ID)
				resp.Body.Close()
			}
		}(w)
	}

	// 4. Start Ingestion HTTP API Service
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.POST("/v1/events", func(c *gin.Context) {
		var event models.Event
		if err := c.ShouldBindJSON(&event); err != nil {
			return
		}
		event.CreatedAt = time.Now()
		_ = kafka.BrokerInstance.PublishRawEvent(event)
		c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
	})

	fmt.Println("\n[API]: Ingestion Server running live on http://localhost:8082")
	_ = r.Run(":8082")
}

func mustParseURL(raw string) *url.URL {
	u, _ := url.Parse(raw)
	return u
}
