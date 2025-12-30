package main

import (
	"github.com/devlibx/gox-helix/examples/integration/code"
	"time"
)

func main() {

	nodeCount := 2
	code.DeleteData = false

	for i := 0; i < nodeCount; i++ {
		go func(index int) {
			code.FullMain()
		}(i)
	}
	time.Sleep(1 * time.Hour)
}
