package util

import (
	"github.com/devlibx/gox-base/v2"
	"sync"
	"time"
)

// MockCrossFunction provides a controllable time service for testing
type MockCrossFunction struct {
	gox.CrossFunction
	startTime    time.Time     // Real time when mock was created
	mockTime     time.Time     // Initial mock time
	acceleration int           // Time acceleration factor (e.g., 10 for 10x speed)
	mutex        sync.RWMutex
}

func NewMockCrossFunction(initialTime time.Time) *MockCrossFunction {
	return &MockCrossFunction{
		CrossFunction: gox.NewNoOpCrossFunction(),
		startTime:     time.Now(),
		mockTime:      initialTime,
		acceleration:  10, // 10x time acceleration
	}
}

func (m *MockCrossFunction) Now() time.Time {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	// Calculate accelerated time based on real time elapsed
	realElapsed := time.Since(m.startTime)
	acceleratedElapsed := time.Duration(int64(realElapsed) * int64(m.acceleration))
	return m.mockTime.Add(acceleratedElapsed)
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
	// Sleep for real duration divided by acceleration factor
	actualSleep := time.Duration(int64(d) / int64(m.acceleration))
	time.Sleep(actualSleep)
}
