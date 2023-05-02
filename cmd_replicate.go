package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
)

type cmdReplicate struct {
	srcDB  *mssql
	dstDB  *postgres
	metaDB *sqlite

	tablesToPutLast []string
	excludeTables   []string

	initialCopyBatchSize        uint
	changeTrackingRetentionDays uint

	changeTrackingCopyMinInterval time.Duration

	allInitialCopyDone bool

	metricsClient *metricsClient
}

func newCmdReplicate(
	srcDB *mssql,
	dstDB *postgres,
	metaDB *sqlite,
	tablesToPutLast []string,
	excludeTables []string,
	initialCopyBatchSize uint,
	changeTrackingRetentionDays uint,
	metricsClient *metricsClient,
) *cmdReplicate {
	return &cmdReplicate{
		srcDB:  srcDB,
		dstDB:  dstDB,
		metaDB: metaDB,

		tablesToPutLast: tablesToPutLast,
		excludeTables:   excludeTables,

		initialCopyBatchSize:        initialCopyBatchSize,
		changeTrackingRetentionDays: changeTrackingRetentionDays,

		changeTrackingCopyMinInterval: 1 * time.Minute,

		metricsClient: metricsClient,
	}
}

func (cmd *cmdReplicate) start(ctx context.Context) error {
	if err := stashDstForeignKeys(cmd.dstDB, cmd.metaDB); err != nil {
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
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("aborted: %w", err)
		}

		if err := cmd.srcDB.enableChangeTracking(t, cmd.changeTrackingRetentionDays); err != nil {
			return fmt.Errorf("enable change tracking: %w", err)
		}

		// Auto retry initial table copy error
		for {
			err := cmd.copyInitial(ctx, t)
			if err == nil {
				break
			}

			if errors.Is(err, context.Canceled) {
				return fmt.Errorf("initial table copy: %w", err)
			}

			log.Err(err).Msgf("Initial table copy error (will retry)")

			select {
			case <-time.After(5 * time.Minute):
			case <-ctx.Done():
				return fmt.Errorf("initial table copy: %w", ctx.Err())
			}
		}

		log.Info().Msgf("[%s] Initial table copy complete, tracking changes in background...", t)
		ctQueue <- t
	}

	cmd.allInitialCopyDone = true
	<-wait

	return nil
}

func stashDstForeignKeys(dstDB *postgres, metaDB *sqlite) error {
	ok, err := metaDB.hasSavedForeignKeys()
	if err != nil {
		return fmt.Errorf("check saved foreign keys: %w", err)
	}
	if ok {
		return nil
	}

	fks, err := dstDB.getForeignKeys()
	if err != nil {
		return fmt.Errorf("get foreign keys: %w", err)
	}
	if len(fks) == 0 {
		return nil
	}

	if err := metaDB.insertSavedForeignKeys(fks); err != nil {
		return fmt.Errorf("save foreign keys: %w", err)
	}

	if err := dstDB.dropForeignKeys(fks); err != nil {
		return fmt.Errorf("drop foreign keys: %w", err)
	}

	return nil
}

type rowData map[string]any

type tableInfo struct {
	schema string
	name   string
}

func (t tableInfo) String() string {
	return t.schema + "." + t.name
}

func (cmd *cmdReplicate) copyInitial(ctx context.Context, t tableInfo) error {
	done, lastInsertedID, err := cmd.metaDB.getInitialCopyStatus(t)
	if err != nil {
		return fmt.Errorf("get last initial copy progress: %w", err)
	}

	if done {
		log.Info().Msgf("[%s] Initial table copy already done.", t)
		return nil
	}

	ver, err := cmd.srcDB.getChangeTrackingCurrentVersion(t)
	if err != nil {
		return fmt.Errorf("get change tracking current version: %w", err)
	}

	if err := cmd.metaDB.upsertChangeTrackingVersion(t, ver); err != nil {
		return fmt.Errorf("save change tracking current version: %w", err)
	}

	pks, err := cmd.dstDB.getPrimaryKeys(dstTable(t))
	if err != nil {
		return fmt.Errorf("get table primary keys: %w", err)
	}

	// Only support resumable insert if table has single column PK.
	// See the note inside cmd.srcDB.readRows()
	var lastCopiedID any
	if len(pks) == 1 && lastInsertedID != "" {
		lastCopiedID = lastInsertedID
	}

	log.Info().Msgf("[%s] Starting initial table copy...", t)

	if err := stashDstIndexes(cmd.dstDB, cmd.metaDB, t); err != nil {
		return fmt.Errorf("save and drop dst indexes: %w", err)
	}

	srcCount, _ := cmd.srcDB.getRowCount(t)
	dstCount, _ := cmd.dstDB.getRowCount(dstTable(t))
	bar := progressbar.NewOptions64(srcCount,
		progressbar.OptionThrottle(500*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("rows"),
	)
	bar.Add64(dstCount)
	cb := func() {
		bar.Add64(1)
	}

	var (
		rowChan        = make(chan rowData)
		errChan        = make(chan error, 2)
		newCtx, cancel = context.WithCancel(ctx)
		wg             = &sync.WaitGroup{}
	)
	wg.Add(2)

	go func() {
		if err := cmd.srcDB.readRowsWithPK(newCtx, t, lastCopiedID, rowChan); err != nil {
			errChan <- err
			cancel()
		}
		close(rowChan)
		wg.Done()
	}()

	go func() {
		lastID, err := cmd.dstDB.insertRows(newCtx, dstTable(t), lastCopiedID == nil, cmd.initialCopyBatchSize, rowChan, cb)
		if err != nil {
			errChan <- err
			cancel()
		}

		if lastID != nil {
			if err := cmd.metaDB.updateInitialCopyLastID(t, lastID); err != nil {
				log.Err(err).Msgf("[%s] Error saving last inserted ID to meta DB.", t)
			}
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

	if err := cmd.metaDB.markInitialCopyDone(t); err != nil {
		return fmt.Errorf("mark initial copy done: %w", err)
	}

	return nil
}

func stashDstIndexes(dstDB *postgres, metaDB *sqlite, t tableInfo) error {
	ixs, err := dstDB.getIndexes(t)
	if err != nil {
		return fmt.Errorf("get indexes: %w", err)
	}

	if err := metaDB.insertSavedIndexes(ixs); err != nil {
		return fmt.Errorf("save index: %w", err)
	}

	if err := dstDB.dropIndexes(ixs); err != nil {
		return fmt.Errorf("drop indexes: %w", err)
	}

	return nil
}

func restoreDstIndexes(ctx context.Context, dstDB *postgres, metaDB *sqlite, t tableInfo) error {
	ixs, err := metaDB.getSavedIndexes(t)
	if err != nil {
		return fmt.Errorf("get dst indexes from meta DB: %w", err)
	}

	if err := dstDB.createIndexes(ctx, ixs); err != nil {
		return fmt.Errorf("create indexes in dst DB: %w", err)
	}

	if err := metaDB.truncateSavedIndexes(t); err != nil {
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

type rowChange struct {
	operation   string
	primaryKeys rowData
	rowdata     rowData
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
		rowChan        = make(chan rowChange)
		errChan        = make(chan error, 2)
		newCtx, cancel = context.WithCancel(ctx)
		wg             = &sync.WaitGroup{}
	)
	wg.Add(2)

	go func() {
		n, err := cmd.srcDB.readRowChanges(newCtx, t, ver, rowChan)
		if err != nil {
			errChan <- err
			cancel()
		}
		close(rowChan)
		nRead = n
		wg.Done()
	}()

	go func() {
		n, err := cmd.dstDB.writeRowChanges(newCtx, dstTable(t), rowChan, func(tc *rowChange) {
			cmd.metricsClient.changesReplicated.WithLabelValues(t.schema+"."+t.name, tc.operation).Inc()
		})

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

	if err := cmd.metaDB.upsertChangeTrackingVersion(t, newVer); err != nil {
		return 0, 0, fmt.Errorf("save change tracking version: %w", err)
	}

	return nRead, nWrite, nil
}

var errInvalidLastSyncVersion = fmt.Errorf("min valid version is newer than last sync version")

func (cmd *cmdReplicate) getLastValidSyncVersion(t tableInfo) (int64, error) {
	lastVer, err := cmd.metaDB.getChangeTrackingLastVersion(t)
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

func dstTable(srcTable tableInfo) tableInfo {
	ret := tableInfo{
		schema: strings.ToLower(srcTable.schema),
		name:   strings.ToLower(srcTable.name),
	}

	if ret.schema == "dbo" {
		ret.schema = "public"
	}

	return ret
}
