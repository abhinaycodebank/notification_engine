package main

import (
	"fmt"
	"net/http"
	"time"

	// "notification-engine/internal/config"
	"notification-engine/internal/kafka"
	"notification-engine/internal/models"

	"github.com/gin-gonic/gin"
)

func main() {
	// cfg := config.Load()

	// Use a different port than subscription-api to prevent network port conflicts
	ingestPort := "8082"

	r := gin.Default()

	r.POST("/v1/events", func(c *gin.Context) {
		var event models.Event
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload layout"})
			return
		}

		// Enforce required fields
		if event.ID == "" || event.EventType == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Fields 'id' and 'event_type' are structural requirements"})
			return
		}

		// Inject system-level event lifecycle metadata
		event.CreatedAt = time.Now()

		// Stream event straight into Kafka
		err := kafka.BrokerInstance.PublishRawEvent(event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to stream event to message broker"})
			return
		}

		// At-Least-Once Delivery: We instantly send a 202 Accepted.
		// Processing runs asynchronously down the pipe.
		c.JSON(http.StatusAccepted, gin.H{
			"status":   "accepted",
			"event_id": event.ID,
		})
	})

	fmt.Printf("High-Throughput Ingest API listening closely on port %s...\n", ingestPort)
	r.Run(":" + ingestPort)
}
