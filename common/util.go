package common

import (
	"bytes"
	"encoding/gob"
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

func EncodeToBytes[StructType any](obj StructType) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeFromBytes[StructType any](data []byte) (StructType, error) {
	var obj StructType
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(&obj)
	if err != nil {
		return obj, err
	}
	return obj, nil
}
