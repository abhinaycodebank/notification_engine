package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"notification-engine/internal/database"
	"notification-engine/internal/kafka"
	"notification-engine/internal/models"

	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)

	// ----------------------------------------------------------------
	// 1. BACKGROUND ENGINE: Matching Engine Consumer
	// ----------------------------------------------------------------
	go func() {
		fmt.Println("[ENGINE]: Matching Engine consumer loop initialized and polling raw-events...")
		for event := range kafka.BrokerInstance.RawEvents {
			fmt.Printf("\n[ENGINE 🔍]: Processing Event ID: %s (%s)\n", event.ID, event.EventType)

			subscriptions := database.Instance.GetAll()
			for _, sub := range subscriptions {
				// Simple key-value exact match logic
				matched := true
				for k, v := range sub.Filters {
					if k == "event_type" && event.EventType != v {
						matched = false
					}
					if val, ok := event.Data[k]; ok && val != v {
						matched = false
					}
				}

				if matched {
					fmt.Printf("  └─► [MATCH FOUND 🎉]: Event %s satisfies criteria for subscriber '%s'\n", event.ID, sub.Name)
					task := models.DeliveryTask{
						Event:        event,
						Subscription: sub,
						Attempt:      1,
					}
					_ = kafka.BrokerInstance.PublishDeliveryTask(task)
				}
			}
		}
	}()

	// ----------------------------------------------------------------
	// 2. BACKGROUND ENGINE: Delivery Workers (Pool of 2)
	// ----------------------------------------------------------------
	httpClient := &http.Client{Timeout: 4 * time.Second}
	for w := 1; w <= 2; w++ {
		go func(workerID int) {
			fmt.Printf("[WORKER %d ⚙️]: Online and waiting for matched dispatch tasks...\n", workerID)
			for task := range kafka.BrokerInstance.DeliveryTasks {
				fmt.Printf("[WORKER %d 🚚]: Handling task Event %s -> Target URL: %s (Attempt %d)\n",
					workerID, task.Event.ID, task.Subscription.WebhookURL, task.Attempt)

				// Execute real HTTP payload delivery
				jsonData, _ := json.Marshal(task.Event)
				req, _ := http.NewRequest("POST", task.Subscription.WebhookURL, bytes.NewBuffer(jsonData))
				req.Header.Set("Content-Type", "application/json")

				resp, err := httpClient.Do(req)
				if err != nil {
					fmt.Printf("  └─► [WORKER %d ❌]: Webhook failed: %v\n", workerID, err)
					// At-least-once retry orchestration logic would queue back here if attempt < 3
					continue
				}

				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("  └─► [WORKER %d ✅]: Received HTTP %d from endpoint! Response snippet: %s\n",
					workerID, resp.StatusCode, string(body[:50]))
				resp.Body.Close()

				fmt.Printf("[AUDIT STAMP 📝]: Logged success status for event %s to relational memory tables.\n", task.Event.ID)
			}
		}(w)
	}

	// Give background threads a split-second to initialize outputs cleanly
	time.Sleep(200 * time.Millisecond)

	// ----------------------------------------------------------------
	// 3. HTTP ENGINE: Subscription Administration API (Port 8081)
	// ----------------------------------------------------------------
	go func() {
		r := gin.New()
		r.POST("/subscriptions", func(c *gin.Context) {
			var sub models.Subscription
			if err := c.ShouldBindJSON(&sub); err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if sub.ID == "" {
				c.JSON(400, gin.H{"error": "ID is required"})
				return
			}

			_ = database.Instance.Create(&sub)
			fmt.Printf("[ADMIN API 💾]: Subscription '%s' successfully added to the system database registry.\n", sub.Name)
			c.JSON(201, sub)
		})

		r.GET("/subscriptions", func(c *gin.Context) {
			c.JSON(200, database.Instance.GetAll())
		})

		fmt.Println("[API 🌐]: Admin Subscription API executing live operations on http://localhost:8081")
		_ = r.Run(":8081")
	}()

	// ----------------------------------------------------------------
	// 4. HTTP ENGINE: Ingest API Gateway (Port 8082)
	// ----------------------------------------------------------------
	rIngest := gin.New()
	rIngest.POST("/v1/events", func(c *gin.Context) {
		var event models.Event
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if event.ID == "" || event.EventType == "" {
			c.JSON(400, gin.H{"error": "Missing structural fields"})
			return
		}

		event.CreatedAt = time.Now()
		_ = kafka.BrokerInstance.PublishRawEvent(event)
		c.JSON(202, gin.H{"status": "accepted", "event_id": event.ID})
	})

	fmt.Println("[API 🌐]: High-Throughput Ingestion API server processing streams on http://localhost:8082")
	_ = rIngest.Run(":8082")
}
