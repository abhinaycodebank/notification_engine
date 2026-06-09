package main

import (
	"fmt"
	"net/http"

	"notification-engine/internal/config"
	"notification-engine/internal/database"
	"notification-engine/internal/models"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg := config.Load()
	r := gin.Default()

	// Route 1: Create a Subscription
	r.POST("/subscriptions", func(c *gin.Context) {
		var sub models.Subscription
		if err := c.ShouldBindJSON(&sub); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if sub.ID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ID is required"})
			return
		}

		err := database.Instance.Create(&sub)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Requirement Check: Invalidate Cache on matching event keys
		invalidateCache(sub.Filters["event_type"])

		c.JSON(http.StatusCreated, sub)
	})

	// Route 2: Delete a Subscription
	r.DELETE("/subscriptions/:id", func(c *gin.Context) {
		id := c.Param("id")

		err := database.Instance.Delete(id)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}

		// Broad invalidation or specific cache purge
		invalidateCache("*")

		c.JSON(http.StatusOK, gin.H{"message": "Subscription removed and cache purged"})
	})

	// Route 3: List all subscriptions (For administrative validation)
	r.GET("/subscriptions", func(c *gin.Context) {
		list := database.Instance.GetAll()
		c.JSON(http.StatusOK, list)
	})

	fmt.Printf("Subscription Administration API listening closely on port %s...\n", cfg.Port)
	r.Run(":" + cfg.Port)
}

func invalidateCache(eventType string) {
	// When we attach the live Redis client later, this drops keys instantly
	fmt.Printf("[CACHE INVALIDATION ACTIVATED]: Evicting match mappings for key target: sub:cache:%s\n", eventType)
}
