package main

import (
	"encoding/json"
	"os"
)

func initConfig() error {
	f, err := os.Create("mssql2pg.json")
	if err != nil {
		return err
	}
	defer f.Close()

	ft, err := os.Create("mssql2pg_test.json")
	if err != nil {
		return err
	}
	defer ft.Close()

	b, _ := json.MarshalIndent(config{}, "", "\t")
	f.Write(b)
	ft.Write(b)

	return nil
}
