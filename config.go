package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type config struct {
	SourceDatabaseUser string `json:"sourceDatabaseUser"`
	SourceDatabasePass string `json:"sourceDatabasePass"`
	SourceDatabaseHost string `json:"sourceDatabaseHost"`
	SourceDatabaseName string `json:"sourceDatabaseName"`

	DestinationDatabaseUser string `json:"destinationDatabaseUser"`
	DestinationDatabasePass string `json:"destinationDatabasePass"`
	DestinationDatabaseHost string `json:"destinationDatabaseHost"`
	DestinationDatabaseName string `json:"destinationDatabaseName"`

	MetaDatabasePath string `json:"metaDatabasePath"`

	TablesToPutLast []string `json:"tablesToPutLast"`
	ExcludeTables   []string `json:"excludeTables"`

	InitialCopyBatchSize        uint `json:"initialCopyBatchSize"`
	ChangeTrackingRetentionDays uint `json:"changeTrackingRetentionDays"`
}

func loadConfig(configPath string) (config, error) {
	f, err := os.Open(configPath)
	if err != nil {
		return config{}, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	var ret config
	if err := json.NewDecoder(f).Decode(&ret); err != nil {
		return config{}, fmt.Errorf("parse file: %w", err)
	}

	return ret, nil
}
