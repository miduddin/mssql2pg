package main

import (
	"fmt"
)

type cmdRestoreFKs struct {
	dstDB  *postgres
	metaDB *sqlite
}

func (cmd *cmdRestoreFKs) start() error {
	fks, err := cmd.metaDB.getSavedForeignKeys()
	if err != nil {
		return fmt.Errorf("get saved foreign keys: %w", err)
	}

	if err := cmd.dstDB.createForeignKeys(fks); err != nil {
		return fmt.Errorf("create foreign keys: %w", err)
	}

	return nil
}
