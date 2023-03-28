package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type cmdReplicate struct {
	srcDB  *sourceDB
	dstDB  *destinationDB
	metaDB *metaDB

	tablesToPutLast []string
	excludeTables   []string

	changeTrackingCopyMinInterval time.Duration

	allInitialCopyDone bool
}

func newCmdReplicate(srcDB *sourceDB, dstDB *destinationDB, metaDB *metaDB, tablesToPutLast, excludeTables []string) *cmdReplicate {
	return &cmdReplicate{
		srcDB:  srcDB,
		dstDB:  dstDB,
		metaDB: metaDB,

		tablesToPutLast: tablesToPutLast,
		excludeTables:   excludeTables,

		changeTrackingCopyMinInterval: 1 * time.Minute,
	}
}

func (cmd *cmdReplicate) start(ctx context.Context) error {
	if err := cmd.saveAndDropDstForeignKeys(); err != nil {
		return fmt.Errorf("backup & drop foreign keys: %w", err)
	}

	var (
		ctQueue         = make(chan tableInfo, 1000)
		ctCtx, ctCancel = context.WithCancel(ctx)
		wait            = make(chan struct{})
	)
	defer func() {
		ctCancel()
		<-wait
	}()

	go func() {
		if err := cmd.copyChangeTracking(ctCtx, ctQueue); err != nil {
			log.Err(err).Msg("Change tracking copy aborted.")
		}
		close(wait)
	}()

	tables, err := cmd.srcDB.getTables(cmd.tablesToPutLast, cmd.excludeTables)
	if err != nil {
		return fmt.Errorf("list src tables: %w", err)
	}

	for _, t := range tables {
		if err := cmd.srcDB.enableChangeTracking(t); err != nil {
			return fmt.Errorf("enable change tracking: %w", err)
		}

		ver, err := cmd.srcDB.getChangeTrackingCurrentVersion(t)
		if err != nil {
			return fmt.Errorf("get change tracking current version: %w", err)
		}

		if err := cmd.metaDB.saveChangeTrackingVersion(t, ver); err != nil {
			return fmt.Errorf("save change tracking current version: %w", err)
		}

		if err := cmd.copyInitial(ctx, t); err != nil {
			return fmt.Errorf("initial table copy: %w", err)
		}

		log.Info().Msgf("[%s] Initial table copy complete, tracking changes in background...", t)
		ctQueue <- t
	}

	cmd.allInitialCopyDone = true
	<-wait

	return nil
}

func (cmd *cmdReplicate) saveAndDropDstForeignKeys() error {
	ok, err := cmd.metaDB.hasSavedForeignKeys()
	if err != nil {
		return fmt.Errorf("check saved foreign keys: %w", err)
	}
	if ok {
		return nil
	}

	fks, err := cmd.dstDB.getForeignKeys()
	if err != nil {
		return fmt.Errorf("get foreign keys: %w", err)
	}
	if len(fks) == 0 {
		return nil
	}

	if err := cmd.metaDB.saveForeignKeys(fks); err != nil {
		return fmt.Errorf("save foreign keys: %w", err)
	}

	if err := cmd.dstDB.dropForeignKeys(fks); err != nil {
		return fmt.Errorf("drop foreign keys: %w", err)
	}

	return nil
}

type rowdata map[string]any

type tableInfo struct {
	schema string
	name   string
}

func (t tableInfo) String() string {
	return t.schema + "." + t.name
}

func (cmd *cmdReplicate) copyInitial(ctx context.Context, t tableInfo) error {
	done, err := cmd.metaDB.getInitialCopyProgress(t)
	if err != nil {
		return fmt.Errorf("get last initial copy progress: %w", err)
	}

	if done {
		log.Info().Msgf("[%s] Initial table copy already done.", t)
		return nil
	}

	log.Info().Msgf("[%s] Starting initial table copy...", t)

	if err := cmd.saveAndDropDstIndexes(t); err != nil {
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
		if err := cmd.srcDB.readRows(newCtx, t, rowChan); err != nil {
			errChan <- err
			cancel()
		}
		close(rowChan)
		wg.Done()
	}()

	go func() {
		if err := cmd.dstDB.insertRows(newCtx, cmd.dstTable(t), rowChan); err != nil {
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
	if err := cmd.restoreDstIndexes(ctx, t); err != nil {
		return fmt.Errorf("restore dst indexes: %w", err)
	}

	if err := cmd.metaDB.markInitialCopyDone(t); err != nil {
		return fmt.Errorf("mark initial copy done: %w", err)
	}

	return nil
}

func (cmd *cmdReplicate) saveAndDropDstIndexes(t tableInfo) error {
	ixs, err := cmd.dstDB.getIndexes(t)
	if err != nil {
		return fmt.Errorf("get indexes: %w", err)
	}

	if err := cmd.metaDB.saveDstIndexes(ixs); err != nil {
		return fmt.Errorf("save index: %w", err)
	}

	if err := cmd.dstDB.dropIndexes(ixs); err != nil {
		return fmt.Errorf("drop indexes: %w", err)
	}

	return nil
}

func (cmd *cmdReplicate) restoreDstIndexes(ctx context.Context, t tableInfo) error {
	ixs, err := cmd.metaDB.getDstIndexes(t)
	if err != nil {
		return fmt.Errorf("get dst indexes from meta DB: %w", err)
	}

	if err := cmd.dstDB.createIndexes(ctx, ixs); err != nil {
		return fmt.Errorf("create indexes in dst DB: %w", err)
	}

	if err := cmd.metaDB.deleteDstIndexes(t); err != nil {
		return fmt.Errorf("delete saved indexse in meta DB: %w", err)
	}

	return nil
}

func (cmd *cmdReplicate) copyChangeTracking(ctx context.Context, queue chan tableInfo) error {
	for {
		select {
		case table := <-queue:
			nr, nw, err := cmd.copyCurrentChangeTracking(ctx, table)

			if errors.Is(err, errInvalidLastSyncVersion) {
				log.Error().Msgf("[%s] Aborting change tracking copy (invalid last sync version).", table)
				continue
			} else if err != nil {
				log.Err(err).Msgf("[%s] Copy change tracking failed.", table)
			} else {
				if cmd.allInitialCopyDone && (nr > 0 || nw > 0) {
					log.Info().Msgf("[%s] Copied change tracking data, read: %d, written: %d", table, nr, nw)
				}
			}

			go func() {
				select {
				case <-time.After(cmd.changeTrackingCopyMinInterval):
					queue <- table
				case <-ctx.Done():
				}
			}()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (cmd *cmdReplicate) copyCurrentChangeTracking(ctx context.Context, t tableInfo) (nRead, nWrite uint, err error) {
	ver, err := cmd.getLastValidSyncVersion(t)
	if err != nil {
		return 0, 0, fmt.Errorf("get last sync version: %w", err)
	}

	newVer, err := cmd.srcDB.getChangeTrackingCurrentVersion(t)
	if err != nil {
		return 0, 0, fmt.Errorf("get change tracking current version: %w", err)
	}

	var (
		rowChan        = make(chan tablechange)
		errChan        = make(chan error, 2)
		newCtx, cancel = context.WithCancel(ctx)
		wg             = &sync.WaitGroup{}
	)
	wg.Add(2)

	go func() {
		n, err := cmd.srcDB.readTableChanges(newCtx, t, ver, rowChan)
		if err != nil {
			errChan <- err
			cancel()
		}
		close(rowChan)
		nRead = n
		wg.Done()
	}()

	go func() {
		n, err := cmd.dstDB.writeTableChanges(newCtx, t, rowChan)
		if err != nil {
			errChan <- err
			cancel()
		}
		nWrite = n
		wg.Done()
	}()

	wg.Wait()

	close(errChan)
	if err := <-errChan; err != nil {
		return 0, 0, fmt.Errorf("read/write changetable: %w", err)
	}

	if err := cmd.metaDB.saveChangeTrackingVersion(t, newVer); err != nil {
		return 0, 0, fmt.Errorf("save change tracking version: %w", err)
	}

	return nRead, nWrite, nil
}

var errInvalidLastSyncVersion = fmt.Errorf("min valid version is newer than last sync version")

func (cmd *cmdReplicate) getLastValidSyncVersion(t tableInfo) (int64, error) {
	lastVer, err := cmd.metaDB.getLastSyncVersion(t)
	if err != nil {
		return 0, fmt.Errorf("get last sync version: %w", err)
	}

	minValidVer, err := cmd.srcDB.getChangeTrackingMinValidVersion(t)
	if err != nil {
		return 0, fmt.Errorf("get min valid version: %w", err)
	}

	if lastVer < minValidVer {
		return 0, fmt.Errorf("%w: %d, min valid version: %d", errInvalidLastSyncVersion, lastVer, minValidVer)
	}

	return lastVer, err
}

func (cmd *cmdReplicate) dstTable(srcTable tableInfo) tableInfo {
	ret := tableInfo{
		schema: strings.ToLower(srcTable.schema),
		name:   strings.ToLower(srcTable.name),
	}

	if ret.schema == "dbo" {
		ret.schema = "public"
	}

	return ret
}
