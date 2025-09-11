package util

import (
	"github.com/devlibx/gox-base/v2"
	"sync"
	"time"
)

// MockCrossFunction provides a controllable time service for testing
type MockCrossFunction struct {
	gox.CrossFunction
	startTime    time.Time // Real time when mock was created
	mockTime     time.Time // Initial mock time
	acceleration int       // Time acceleration factor (e.g., 10 for 10x speed)
	lastTime     time.Time // Last time returned by Now() to ensure monotonic behavior
	mutex        sync.RWMutex
}

func NewMockCrossFunction(initialTime time.Time) *MockCrossFunction {
	return &MockCrossFunction{
		CrossFunction: gox.NewNoOpCrossFunction(),
		startTime:     time.Now(),
		mockTime:      initialTime,
		lastTime:      initialTime,
		acceleration:  10, // 10x time acceleration
	}
}

func (m *MockCrossFunction) Now() time.Time {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Calculate accelerated time based on real time elapsed
	realElapsed := time.Since(m.startTime)
	acceleratedElapsed := time.Duration(int64(realElapsed) * int64(m.acceleration))
	currentTime := m.mockTime.Add(acceleratedElapsed)

	// Ensure monotonic behavior - never return a time earlier than last returned time
	// If current time would be the same or earlier, advance by a small amount
	if !currentTime.After(m.lastTime) {
		currentTime = m.lastTime.Add(time.Nanosecond)
	}

	m.lastTime = currentTime
	return currentTime
}

func (m *MockCrossFunction) SetTime(t time.Time) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockTime = t
	m.startTime = time.Now() // Reset the real time baseline
	// Update lastTime if the new time is later to maintain monotonic behavior
	if t.After(m.lastTime) {
		m.lastTime = t
	}
}

func (m *MockCrossFunction) AdvanceTime(duration time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.mockTime = m.mockTime.Add(duration)
	m.startTime = time.Now() // Reset the real time baseline
	// Update lastTime to the new advanced time to maintain monotonic behavior
	newTime := m.mockTime
	if newTime.After(m.lastTime) {
		m.lastTime = newTime
	}
}

func (m *MockCrossFunction) Sleep(d time.Duration) {
	// Sleep for real duration divided by acceleration factor
	actualSleep := time.Duration(int64(d) / int64(m.acceleration))
	time.Sleep(actualSleep)
}

// NormalizeDuration converts a real duration to accelerated duration for timeout calculations
// With 10x acceleration: 30s becomes 3s in accelerated time, so timeouts work correctly
func (m *MockCrossFunction) NormalizeDuration(realDuration time.Duration) time.Duration {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	// Convert real duration to accelerated duration by dividing by acceleration factor
	// This ensures timeout logic works correctly with accelerated time
	return time.Duration(int64(realDuration) / int64(m.acceleration))
}
