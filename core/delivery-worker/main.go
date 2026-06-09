package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"notification-engine/internal/kafka"
	"notification-engine/internal/models"
)

const (
	MaxAttempts = 3
	WorkerCount = 5 // Number of concurrent delivery goroutines
)

func main() {
	fmt.Printf("Delivery Engine online. Spawning %d concurrent dispatch workers...\n", WorkerCount)

	// Build a shared standard safe client configured with execution timeouts
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Spin up the worker thread pool pool
	for i := 1; i <= WorkerCount; i++ {
		go startWorker(i, httpClient)
	}

	// Keep main thread alive safely
	select {}
}

func startWorker(workerID int, client *http.Client) {
	fmt.Printf("[WORKER %d]: Ready and polling delivery-tasks queue...\n", workerID)

	for task := range kafka.BrokerInstance.DeliveryTasks {
		fmt.Printf("[WORKER %d]: Processing dispatch task for Sub '%s' (Attempt %d/%d)\n",
			workerID, task.Subscription.Name, task.Attempt, MaxAttempts)

		executeDelivery(task, client)
	}
}

func executeDelivery(task models.DeliveryTask, client *http.Client) {
	// Marshal the event inner payload block data to pass to subscriber
	jsonData, err := json.Marshal(task.Event)
	if err != nil {
		fmt.Printf("  └─► [FATAL]: Failed to marshal payload data for Event ID: %s\n", task.Event.ID)
		return
	}

	req, err := http.NewRequest("POST", task.Subscription.WebhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logAudit(task, 0, "Invalid URL layout metadata setup structured config", "failed")
		return
	}
	req.Header.Set("Content-Type", applicationJSONHeader())

	// Execute HTTP call
	startTime := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(startTime)

	// Handle standard network Layer connection drops / Target server down timeouts
	if err != nil {
		fmt.Printf("  └─► [NETWORK ERROR]: Connection failed to %s after %v: %v\n", task.Subscription.WebhookURL, duration, err)
		handleFailure(task, 0, err.Error())
		return
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	responseBodyString := string(bodyBytes)

	// Check HTTP payload routing response statuses
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Printf("  └─► [SUCCESS]: Delivered successfully to %s. Status: %d in %v\n", task.Subscription.WebhookURL, resp.StatusCode, duration)
		logAudit(task, resp.StatusCode, responseBodyString, "delivered")
	} else {
		fmt.Printf("  └─► [SERVER ERROR]: Target returned status %d. Content: %s\n", resp.StatusCode, responseBodyString)
		handleFailure(task, resp.StatusCode, responseBodyString)
	}
}

func handleFailure(task models.DeliveryTask, statusCode int, responseBody string) {
	// If we haven't exhausted our attempt pool threshold, schedule a retry
	if task.Attempt < MaxAttempts {
		task.Attempt++

		// In production, calculating an exponential back-off wait happens here
		// e.g., math.Pow(2, attempt) seconds.
		// For our mock pipeline, we re-queue it into the channel queue broker immediately.
		fmt.Printf("    └─► [RETRY QUEUED]: Scheduling dispatch task attempt %d downstream...\n", task.Attempt)
		_ = kafka.BrokerInstance.PublishDeliveryTask(task)

		logAudit(task, statusCode, responseBody, "pending_retry")
	} else {
		fmt.Printf("    └─► [FAILED PERMANENTLY]: Max retries exhausted for Event %s targeting Sub %s\n", task.Event.ID, task.Subscription.ID)
		logAudit(task, statusCode, responseBody, "failed")
	}
}

func logAudit(task models.DeliveryTask, statusCode int, responseBody string, status string) {
	// This mirrors our database structural audit pattern logs
	fmt.Printf("[AUDIT TRAIL]: Logging to Postgres DB -> Event: %s, Sub: %s, Attempt: %d, Status: %s, Code: %d\n",
		task.Event.ID, task.Subscription.Name, task.Attempt, status, statusCode)
}

func applicationJSONHeader() string {
	return "application/json"
}
