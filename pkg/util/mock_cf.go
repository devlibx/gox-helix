package util

import (
	"github.com/devlibx/gox-base/v2"
	"sync"
	"time"
)

// MockCrossFunction provides a controllable time service for testing
type MockCrossFunction struct {
	gox.CrossFunction
	mockTime time.Time
	mutex    sync.RWMutex
}

func NewMockCrossFunction(initialTime time.Time) *MockCrossFunction {
	return &MockCrossFunction{
		CrossFunction: gox.NewNoOpCrossFunction(),
		mockTime:      initialTime,
	}
}

func (m *MockCrossFunction) Now() time.Time {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.mockTime
}

func (m *MockCrossFunction) SetTime(t time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockTime = t
}

func (m *MockCrossFunction) AdvanceTime(duration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockTime = m.mockTime.Add(duration)
}

func (m *MockCrossFunction) Sleep(d time.Duration) {
	ms := d.Milliseconds()
	newMs := ms / 10
	time.Sleep(time.Duration(newMs) * time.Millisecond)
}
