package kafka

import (
	"fmt"
	"sync"

	"notification-engine/internal/models"
)

type MockKafkaBroker struct {
	mu            sync.RWMutex
	RawEvents     chan models.Event
	DeliveryTasks chan models.DeliveryTask
}

// Global broker instance to bridge our streaming pipeline components locally
var BrokerInstance = &MockKafkaBroker{
	RawEvents:     make(chan models.Event, 1000),        // Buffer size for holding items safely
	DeliveryTasks: make(chan models.DeliveryTask, 1000), // Buffer size for holding delivery tasks safely
}

func (m *MockKafkaBroker) PublishRawEvent(event models.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// In a real setup, we partition based on event.EventType.
	// For our interactive run, we print out the routing partition logic.
	fmt.Printf("[KAFKA BROKER]: Routing Event ID %s to Partition group determined by type: '%s'\n", event.ID, event.EventType)

	// Push into channel
	m.RawEvents <- event
	return nil
}

// PublishDeliveryTask pushes a matched notification to the dispatcher queue
func (m *MockKafkaBroker) PublishDeliveryTask(task models.DeliveryTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Printf("[KAFKA BROKER]: Enqueueing Delivery Task for Sub '%s' -> Target: %s\n", task.Subscription.Name, task.Subscription.WebhookURL)
	m.DeliveryTasks <- task
	return nil
}
