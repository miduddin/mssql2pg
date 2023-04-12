package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type cmdFullCopy struct {
	srcDB     *sourceDB
	dstDB     *destinationDB
	metaDB    *metaDB
	batchSize uint
	tables    []string
}

func newCmdFullCopy(srcDB *sourceDB, dstDB *destinationDB, metaDB *metaDB, batchSize uint, tables []string) *cmdFullCopy {
	return &cmdFullCopy{
		srcDB:     srcDB,
		dstDB:     dstDB,
		metaDB:    metaDB,
		batchSize: batchSize,
		tables:    tables,
	}
}

func (cmd *cmdFullCopy) start(ctx context.Context) error {
	tables, err := cmd.srcDB.getTables(cmd.tables, nil)
	if err != nil {
		return fmt.Errorf("get table list from source: %w", err)
	}
	tables = tables[len(tables)-len(cmd.tables):]

	if err := saveAndDropDstForeignKeys(cmd.dstDB, cmd.metaDB); err != nil {
		return fmt.Errorf("backup & drop foreign keys: %w", err)
	}

	for _, t := range tables {
		log.Info().Msgf("[%s] Starting table copy...", t)

		for {
			err := cmd.truncateAndCopy(ctx, t)
			if err == nil {
				break
			}

			if errors.Is(err, context.Canceled) {
				return fmt.Errorf("copy table: %w", err)
			}

			log.Err(err).Msgf("[%s] Error copying table (will retry).", t)
			select {
			case <-ctx.Done():
				return fmt.Errorf("aborted, reason: %w", err)
			case <-time.After(5 * time.Minute):
			}
		}

		log.Info().Msgf("[%s] Table copied.", t)
	}

	return nil
}

func (cmd *cmdFullCopy) truncateAndCopy(ctx context.Context, t tableInfo) error {
	if err := saveAndDropDstIndexes(cmd.dstDB, cmd.metaDB, t); err != nil {
		return fmt.Errorf("save and drop dst indexes: %w", err)
	}

	var (
		rowChan        = make(chan rowdata)
		errChan        = make(chan error, 2)
		newCtx, cancel = context.WithCancel(ctx)
		wg             = &sync.WaitGroup{}
	)
	wg.Add(2)

	go func() {
		if err := cmd.srcDB.readRows(newCtx, t, rowChan, fmt.Sprintf("SELECT * FROM [%s].[%s]", t.schema, t.name)); err != nil {
			errChan <- err
			cancel()
		}
		close(rowChan)
		wg.Done()
	}()

	go func() {
		if _, err := cmd.dstDB.insertRows(newCtx, dstTable(t), true, cmd.batchSize, rowChan); err != nil {
			errChan <- err
			cancel()
		}
		wg.Done()
	}()

	wg.Wait()

	close(errChan)
	if err := <-errChan; err != nil {
		return fmt.Errorf("read/write data: %w", err)
	}

	log.Info().Msgf("[%s] Rebuilding indexes...", t)
	if err := restoreDstIndexes(ctx, cmd.dstDB, cmd.metaDB, t); err != nil {
		return fmt.Errorf("restore dst indexes: %w", err)
	}

	return nil
}
