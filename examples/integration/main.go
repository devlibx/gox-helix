package main

import (
	_ "embed"
	"github.com/devlibx/gox-helix/examples/integration/code"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	code.FullMain()
}
