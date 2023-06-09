package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	log.Logger = log.Output(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.TimeFormat = "Jan 2, 15:04 MST"
	}))
}

func main() {
	if len(os.Args) < 2 {
		help()
		return
	}

	if os.Args[1] == "init_config" {
		initConfig()
		return
	}

	cfg, err := loadConfig("mssql2pg.json")
	exitIfErr(err)

	srcDB, dstDB, metaDB, err := openDatabases(cfg)
	exitIfErr(err)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		log.Info().Msgf("Received signal %v, stopping process...", <-sigs)
		cancel()
	}()

	switch os.Args[1] {
	case "replicate":
		cmd := newCmdReplicate(
			srcDB, dstDB, metaDB,
			cfg.TablesToPutLast,
			cfg.ExcludeTables,
			cfg.InitialCopyBatchSize,
			cfg.ChangeTrackingRetentionDays,
			newMetricsClient(8080),
		)
		log.Err(cmd.start(ctx)).Msg("Done.")

	case "restore_fks":
		fr := cmdRestoreFKs{
			dstDB:  dstDB,
			metaDB: metaDB,
		}

		log.Err(fr.start()).Msg("Done.")

	case "fullcopy":
		cmd := newCmdFullCopy(srcDB, dstDB, metaDB, cfg.InitialCopyBatchSize, cfg.FullCopyTables)
		log.Err(cmd.start(ctx)).Msg("Done.")

	default:
		help()
	}
}

func openDatabases(cfg config) (*mssql, *postgres, *sqlite, error) {
	src, err := newMssql(
		cfg.SourceDatabaseUser,
		cfg.SourceDatabasePass,
		cfg.SourceDatabaseHost,
		cfg.SourceDatabaseName,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open src db: %w", err)
	}

	dst, err := newPostgres(
		cfg.DestinationDatabaseUser,
		cfg.DestinationDatabasePass,
		cfg.DestinationDatabaseHost,
		cfg.DestinationDatabaseName,
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open dst db: %w", err)
	}

	meta, err := newSqlite(cfg.MetaDatabasePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open meta db: %w", err)
	}

	return src, dst, meta, nil
}

func help() {
	fmt.Println(`
SQL Server to PostgreSQL data replication tool.
Usage:
	mssql2pg <subcommand>

Subcommands:
	init_config	Creates empty config file with the name "mssql2pg.json" and "mssql2pg_test.json".
			Will override existing files!
	replicate	Run replication from source SQL Server to destination PostgreSQL.
	restore_fks	Restore foreign keys in destination PostgreSQL that were previously
			dropped by "replicate" command.
	fullcopy	Copy tables from source SQL Server to destination PostgreSQL, truncating
			existing data in the destination tables first.
	`)
}

func exitIfErr(err error) {
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
}
