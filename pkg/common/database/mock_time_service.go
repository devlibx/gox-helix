package databaseCommon

import (
	"github.com/devlibx/gox-base/v2"
	"sync"
	"time"
)

type MockTimeService struct {
	mu          sync.RWMutex
	currentTime time.Time
	gox.TimeService
}

func NewMockTimeService(initialTime time.Time) *MockTimeService {
	return &MockTimeService{
		currentTime: initialTime,
	}
}

func (m *MockTimeService) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentTime
}

func (m *MockTimeService) SetTime(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTime = t
}

func (m *MockTimeService) AdvanceTime(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentTime = m.currentTime.Add(d)
}
