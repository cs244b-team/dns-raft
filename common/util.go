package common

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func InitLogger() {
	// Get environment variable for log level (LOG_LEVEL)
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	// Set log level
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("Invalid log level: %s", logLevel)
	}

	log.SetLevel(level)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}
