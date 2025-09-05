package util

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadDevEnv reads environment variables from .env file at the project root if it exists
func LoadDevEnv() error {
	// Get the current working directory
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get working directory: %w", err)
	}

	// Find the project root by searching upward for go.mod
	projectRoot, err := findProjectRoot(wd)
	if err != nil {
		return fmt.Errorf("failed to find project root: %w", err)
	}

	// Look for .env file in project root
	envPath := filepath.Join(projectRoot, ".env")
	
	// Check if .env file exists
	if _, err := os.Stat(envPath); os.IsNotExist(err) {
		// .env file doesn't exist, return without error
		return nil
	}

	// Open the .env file
	file, err := os.Open(envPath)
	if err != nil {
		return fmt.Errorf("failed to open .env file: %w", err)
	}
	defer file.Close()

	// Read and parse the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse key=value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue // Skip invalid lines
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Set the environment variable only if it's not already set
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("failed to set environment variable %s: %w", key, err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading .env file: %w", err)
	}

	return nil
}

// findProjectRoot searches upward from the given directory to find the project root
// by looking for a go.mod file
func findProjectRoot(startDir string) (string, error) {
	currentDir := startDir
	
	for {
		// Check if go.mod exists in current directory
		goModPath := filepath.Join(currentDir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			return currentDir, nil
		}
		
		// Move to parent directory
		parentDir := filepath.Dir(currentDir)
		
		// If we've reached the root directory, stop searching
		if parentDir == currentDir {
			break
		}
		
		currentDir = parentDir
	}
	
	return "", fmt.Errorf("go.mod not found in any parent directory of %s", startDir)
}
