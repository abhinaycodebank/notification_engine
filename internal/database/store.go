package database

import (
	"errors"
	"sync"
	"time"

	"notification-engine/internal/models"
)

// Simple in-memory thread-safe mock store representing Postgres tables
type SubscriptionStore struct {
	mu           sync.RWMutex
	subs         map[string]models.Subscription
	deliveryLogs []models.DeliveryAudit
}

var Instance = &SubscriptionStore{
	subs: make(map[string]models.Subscription),
}

func (s *SubscriptionStore) Create(sub *models.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub.CreatedAt = time.Now()
	sub.UpdatedAt = time.Now()
	s.subs[sub.ID] = *sub
	return nil
}

func (s *SubscriptionStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.subs[id]; !exists {
		return errors.New("subscription not found")
	}
	delete(s.subs, id)
	return nil
}

func (s *SubscriptionStore) GetAll() []models.Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]models.Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		list = append(list, sub)
	}
	return list
}
