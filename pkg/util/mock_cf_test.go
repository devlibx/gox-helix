package util

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMockCrossFunction_BasicFunctionality(t *testing.T) {
	t.Run("NewMockCrossFunction initializes correctly", func(t *testing.T) {
		initialTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
		mock := NewMockCrossFunction(initialTime)
		
		if mock.mockTime != initialTime {
			t.Errorf("Expected mockTime %v, got %v", initialTime, mock.mockTime)
		}
		
		if mock.acceleration != 10 {
			t.Errorf("Expected acceleration 10, got %d", mock.acceleration)
		}
		
		// startTime should be close to now
		if time.Since(mock.startTime) > 10*time.Millisecond {
			t.Errorf("startTime should be recent, but was %v ago", time.Since(mock.startTime))
		}
	})
}

func TestMockCrossFunction_NowAdvancement(t *testing.T) {
	t.Run("Now() advances automatically with 10x acceleration", func(t *testing.T) {
		initialTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
		mock := NewMockCrossFunction(initialTime)
		
		// Get initial time
		time1 := mock.Now()
		
		// Sleep for 100ms real time
		time.Sleep(100 * time.Millisecond)
		
		// Get time again
		time2 := mock.Now()
		
		// With 10x acceleration, 100ms real time = 1000ms mock time
		expectedDiff := 1000 * time.Millisecond
		actualDiff := time2.Sub(time1)
		
		// Allow some tolerance for timing precision
		tolerance := 50 * time.Millisecond
		if actualDiff < expectedDiff-tolerance || actualDiff > expectedDiff+tolerance {
			t.Errorf("Expected time advancement ~%v, got %v", expectedDiff, actualDiff)
		}
		
		// Verify it's still based on initial time
		if !time1.After(initialTime) {
			t.Errorf("Expected time1 %v to be after initialTime %v", time1, initialTime)
		}
	})
	
	t.Run("Multiple Now() calls return consistent advancing time", func(t *testing.T) {
		initialTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
		mock := NewMockCrossFunction(initialTime)
		
		times := make([]time.Time, 5)
		for i := 0; i < 5; i++ {
			times[i] = mock.Now()
			time.Sleep(10 * time.Millisecond) // 10ms real = 100ms mock
		}
		
		// Each time should be later than the previous
		for i := 1; i < len(times); i++ {
			if !times[i].After(times[i-1]) {
				t.Errorf("Time %d (%v) should be after time %d (%v)", 
					i, times[i], i-1, times[i-1])
			}
			
			// Difference should be approximately 100ms (10ms * 10x acceleration)
			diff := times[i].Sub(times[i-1])
			expectedDiff := 100 * time.Millisecond
			tolerance := 50 * time.Millisecond
			
			if diff < expectedDiff-tolerance || diff > expectedDiff+tolerance {
				t.Errorf("Expected diff ~%v between consecutive calls, got %v", 
					expectedDiff, diff)
			}
		}
	})
}

func TestMockCrossFunction_Sleep(t *testing.T) {
	t.Run("Sleep() uses 10x acceleration", func(t *testing.T) {
		mock := NewMockCrossFunction(time.Now())
		
		// Test various sleep durations
		testCases := []struct {
			sleepDuration    time.Duration
			expectedRealTime time.Duration
		}{
			{1 * time.Second, 100 * time.Millisecond},
			{500 * time.Millisecond, 50 * time.Millisecond},
			{100 * time.Millisecond, 10 * time.Millisecond},
		}
		
		for _, tc := range testCases {
			t.Run(tc.sleepDuration.String(), func(t *testing.T) {
				start := time.Now()
				mock.Sleep(tc.sleepDuration)
				actualRealTime := time.Since(start)
				
				// Allow some tolerance for timing precision
				tolerance := 20 * time.Millisecond
				if actualRealTime < tc.expectedRealTime-tolerance || 
				   actualRealTime > tc.expectedRealTime+tolerance {
					t.Errorf("Sleep(%v): expected real time ~%v, got %v", 
						tc.sleepDuration, tc.expectedRealTime, actualRealTime)
				}
			})
		}
	})
}

func TestMockCrossFunction_SetTime(t *testing.T) {
	t.Run("SetTime() resets time baseline correctly", func(t *testing.T) {
		initialTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
		mock := NewMockCrossFunction(initialTime)
		
		// Let some time pass
		time.Sleep(50 * time.Millisecond)
		
		// Set to a new time
		newTime := time.Date(2025, 6, 15, 15, 30, 0, 0, time.UTC)
		mock.SetTime(newTime)
		
		// Now() should return time close to newTime (plus any elapsed time)
		current := mock.Now()
		
		// Should be at least the new time
		if current.Before(newTime) {
			t.Errorf("Expected current time %v to be at or after set time %v", 
				current, newTime)
		}
		
		// Should not be too far ahead (max a few seconds even with acceleration)
		maxExpected := newTime.Add(5 * time.Second)
		if current.After(maxExpected) {
			t.Errorf("Expected current time %v to be before %v", current, maxExpected)
		}
	})
}

func TestMockCrossFunction_AdvanceTime(t *testing.T) {
	t.Run("AdvanceTime() works correctly with automatic advancement", func(t *testing.T) {
		initialTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
		mock := NewMockCrossFunction(initialTime)
		
		// Get baseline time
		time1 := mock.Now()
		
		// Advance by 1 hour
		mock.AdvanceTime(1 * time.Hour)
		
		// Get new time immediately
		time2 := mock.Now()
		
		// Should be approximately 1 hour later
		expectedDiff := 1 * time.Hour
		actualDiff := time2.Sub(time1)
		
		// Allow small tolerance for timing precision
		tolerance := 100 * time.Millisecond
		if actualDiff < expectedDiff-tolerance || actualDiff > expectedDiff+tolerance {
			t.Errorf("Expected time advancement ~%v, got %v", expectedDiff, actualDiff)
		}
	})
}

func TestMockCrossFunction_ThreadSafety(t *testing.T) {
	t.Run("Concurrent access is thread-safe", func(t *testing.T) {
		mock := NewMockCrossFunction(time.Now())
		
		var wg sync.WaitGroup
		errors := make(chan error, 100)
		
		// Start multiple goroutines calling Now() concurrently
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				
				for j := 0; j < 10; j++ {
					t1 := mock.Now()
					time.Sleep(1 * time.Millisecond)
					t2 := mock.Now()
					
					if !t2.After(t1) {
						errors <- fmt.Errorf("t2 (%v) should be after t1 (%v)", t2, t1)
						return
					}
				}
			}()
		}
		
		// Concurrent SetTime calls
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(iteration int) {
				defer wg.Done()
				newTime := time.Date(2025, 1, 1, 10+iteration, 0, 0, 0, time.UTC)
				mock.SetTime(newTime)
			}(i)
		}
		
		// Concurrent AdvanceTime calls
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mock.AdvanceTime(1 * time.Minute)
			}()
		}
		
		wg.Wait()
		close(errors)
		
		// Check for any errors
		for err := range errors {
			t.Error(err)
		}
	})
}

func TestMockCrossFunction_AllocationAlgorithmScenario(t *testing.T) {
	t.Run("Simulates partition release timeout scenario", func(t *testing.T) {
		// This test simulates the exact scenario from the soak test:
		// 1. Partition gets "requested-release" status with timestamp
		// 2. Algorithm checks if timeout (10s) has passed
		// 3. Should transition to "unassigned" after timeout
		
		mock := NewMockCrossFunction(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
		
		// Simulate partition getting "requested-release" status
		releaseTime := mock.Now()
		timeout := 10 * time.Second
		
		t.Logf("Partition marked for release at: %v", releaseTime)
		
		// Simulate waiting less than timeout (should NOT timeout)
		time.Sleep(50 * time.Millisecond) // 50ms real = 500ms mock
		currentTime1 := mock.Now()
		shouldTimeout1 := releaseTime.Add(timeout).Before(currentTime1)
		
		t.Logf("After 50ms real (500ms mock): current=%v, should_timeout=%t", 
			currentTime1, shouldTimeout1)
		
		if shouldTimeout1 {
			t.Error("Should NOT timeout after only 500ms mock time")
		}
		
		// Simulate waiting more than timeout (should timeout)
		time.Sleep(950 * time.Millisecond) // 950ms real = 9.5s mock
		// Total: 1000ms real = 10s mock time
		
		currentTime2 := mock.Now()
		shouldTimeout2 := releaseTime.Add(timeout).Before(currentTime2)
		
		t.Logf("After 1000ms real (10s mock): current=%v, should_timeout=%t", 
			currentTime2, shouldTimeout2)
		
		if !shouldTimeout2 {
			t.Error("SHOULD timeout after 10s mock time")
		}
		
		// Verify the time difference is approximately 10 seconds
		timeDiff := currentTime2.Sub(releaseTime)
		t.Logf("Total mock time elapsed: %v", timeDiff)
		
		expectedDiff := 10 * time.Second
		tolerance := 500 * time.Millisecond
		
		if timeDiff < expectedDiff-tolerance || timeDiff > expectedDiff+tolerance {
			t.Errorf("Expected ~%v mock time elapsed, got %v", expectedDiff, timeDiff)
		}
	})
}

func TestMockCrossFunction_RealWorldTiming(t *testing.T) {
	t.Run("Timing matches expected real-world soak test behavior", func(t *testing.T) {
		mock := NewMockCrossFunction(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
		
		// Simulate 1 minute of chaos phase (real time)
		// This should equal 10 minutes of mock time
		start := time.Now()
		mockStart := mock.Now()
		
		// Wait for 200ms real time (2 seconds mock time)
		time.Sleep(200 * time.Millisecond)
		
		realElapsed := time.Since(start)
		mockElapsed := mock.Now().Sub(mockStart)
		
		expectedMockElapsed := time.Duration(int64(realElapsed) * 10)
		tolerance := 100 * time.Millisecond
		
		t.Logf("Real elapsed: %v", realElapsed)
		t.Logf("Mock elapsed: %v", mockElapsed)
		t.Logf("Expected mock elapsed: %v", expectedMockElapsed)
		
		if mockElapsed < expectedMockElapsed-tolerance || 
		   mockElapsed > expectedMockElapsed+tolerance {
			t.Errorf("Expected mock elapsed ~%v, got %v", 
				expectedMockElapsed, mockElapsed)
		}
		
		// Verify 10x acceleration ratio
		ratio := float64(mockElapsed) / float64(realElapsed)
		expectedRatio := 10.0
		ratioTolerance := 1.0 // Allow ±1.0 ratio tolerance
		
		if ratio < expectedRatio-ratioTolerance || ratio > expectedRatio+ratioTolerance {
			t.Errorf("Expected acceleration ratio ~%.1f, got %.1f", 
				expectedRatio, ratio)
		}
	})
}