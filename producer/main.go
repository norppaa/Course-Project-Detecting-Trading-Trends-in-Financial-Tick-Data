package main

import (
	"log"
	"time"
)

func main() {
	log.Println("[Producer] Service initialized. Ready for implementation.")
	// Keep container alive until stopped
	for {
		time.Sleep(time.Hour)
	}
}
