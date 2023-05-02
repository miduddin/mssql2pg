package main

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const metaDBSchema = `
CREATE TABLE IF NOT EXISTS replication_progress (
	schema_name                  TEXT NOT NULL,
	table_name                   TEXT NOT NULL,
	initial_copy_done            INT  NOT NULL DEFAULT 0,
	initial_copy_last_id         TEXT,
	change_tracking_last_version INT  NOT NULL DEFAULT 0,

	PRIMARY KEY (schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS foreign_keys (
	schema_name   TEXT NOT NULL,
	table_name    TEXT NOT NULL,
	fk_name       TEXT NOT NULL,
	fk_definition TEXT NOT NULL,

	PRIMARY KEY (schema_name, table_name, fk_name)
);

CREATE TABLE IF NOT EXISTS indexes (
	schema_name TEXT NOT NULL,
	table_name  TEXT NOT NULL,
	index_name  TEXT NOT NULL,
	index_def   TEXT NOT NULL,

	PRIMARY KEY (schema_name, table_name, index_name)
);
`

type sqlite struct {
	db *sqlx.DB
}

func newSqlite(dbPath string) (*sqlite, error) {
	db, err := sqlx.Connect("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.MustExec(metaDBSchema)

	return &sqlite{db}, nil
}

func (db *sqlite) hasSavedForeignKeys() (bool, error) {
	var count int
	if err := db.db.QueryRowx(`SELECT count(*) FROM foreign_keys`).Scan(&count); err != nil {
		return false, fmt.Errorf("sql select: %w", err)
	}

	return count > 0, nil
}

func (db *sqlite) insertSavedForeignKeys(fks []foreignKey) error {
	data := make([]rowData, len(fks))
	for i, fk := range fks {
		data[i] = rowData{
			"schema_name":   fk.t.schema,
			"table_name":    fk.t.name,
			"fk_name":       fk.name,
			"fk_definition": fk.definition,
		}
	}

	_, err := db.db.NamedExec(
		`INSERT INTO foreign_keys (schema_name, table_name, fk_name, fk_definition)
		VALUES (:schema_name, :table_name, :fk_name, :fk_definition)`,
		data,
	)
	if err != nil {
		return fmt.Errorf("sql insert: %w", err)
	}

	return nil
}

func (db *sqlite) getSavedForeignKeys() ([]foreignKey, error) {
	rows, err := db.db.Queryx(`
		SELECT
			schema_name,
			table_name,
			fk_name,
			fk_definition
		FROM foreign_keys
	`)
	if err != nil {
		return nil, fmt.Errorf("sql select: %w", err)
	}

	ret := []foreignKey{}
	for rows.Next() {
		fk := foreignKey{}
		if err := rows.Scan(&fk.t.schema, &fk.t.name, &fk.name, &fk.definition); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		ret = append(ret, fk)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	return ret, nil
}

func (db *sqlite) getSavedIndexes(t tableInfo) ([]index, error) {
	rows, err := db.db.Queryx(
		`SELECT index_name, index_def
		FROM indexes
		WHERE schema_name = ? AND table_name = ?`,
		t.schema, t.name,
	)
	if err != nil {
		return nil, fmt.Errorf("sql query: %w", err)
	}

	var ret []index
	for rows.Next() {
		var ixn, ixd string
		if err := rows.Scan(&ixn, &ixd); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		ret = append(ret, index{table: t, name: ixn, def: ixd})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows: %w", err)
	}

	return ret, nil
}

func (db *sqlite) insertSavedIndexes(ixs []index) error {
	if len(ixs) == 0 {
		return nil
	}

	data := make([]rowData, len(ixs))
	for i, ix := range ixs {
		data[i] = rowData{
			"schema_name": ix.table.schema,
			"table_name":  ix.table.name,
			"index_name":  ix.name,
			"index_def":   ix.def,
		}
	}

	_, err := db.db.NamedExec(
		`INSERT INTO indexes (schema_name, table_name, index_name, index_def)
		VALUES (:schema_name, :table_name, :index_name, :index_def)
		ON CONFLICT DO NOTHING`,
		data,
	)

	return err
}

func (db *sqlite) truncateSavedIndexes(t tableInfo) error {
	_, err := db.db.Exec(
		`DELETE FROM indexes WHERE schema_name = ? AND table_name = ?`,
		t.schema, t.name,
	)

	return err
}

func (db *sqlite) upsertChangeTrackingVersion(t tableInfo, ver int64) error {
	// Insert first to make sure table data exist.
	db.db.Exec(
		`INSERT INTO replication_progress (schema_name, table_name)
		VALUES (?, ?)`,
		t.schema, t.name,
	)

	_, err := db.db.Exec(
		`UPDATE replication_progress
		SET change_tracking_last_version = ?
		WHERE schema_name = ? AND table_name = ?`,
		ver, t.schema, t.name,
	)

	return err
}

func (db *sqlite) getInitialCopyStatus(t tableInfo) (done bool, lastID string, err error) {
	// Insert first to DB for this table, ignore error if already exist.
	db.db.Exec(
		`INSERT INTO replication_progress (schema_name, table_name) VALUES (?, ?)`,
		t.schema, t.name,
	)

	var ns sql.NullString
	err = db.db.QueryRowx(
		`SELECT initial_copy_done, initial_copy_last_id
		FROM replication_progress
		WHERE schema_name = ? AND table_name = ?`,
		t.schema, t.name,
	).Scan(&done, &ns)
	if err != nil {
		return false, "", fmt.Errorf("sql select: %w", err)
	}

	return done, ns.String, nil
}

func (db *sqlite) updateInitialCopyLastID(t tableInfo, id any) error {
	_, err := db.db.Exec(
		`UPDATE replication_progress
		SET initial_copy_last_id = ?
		WHERE schema_name = ? AND table_name = ?`,
		id, t.schema, t.name,
	)
	return err
}

func (db *sqlite) markInitialCopyDone(t tableInfo) error {
	_, err := db.db.Exec(
		`UPDATE replication_progress
		SET initial_copy_done = 1
		WHERE schema_name = ? AND table_name = ?`,
		t.schema, t.name,
	)
	if err != nil {
		return fmt.Errorf("sql update: %w", err)
	}

	return nil
}

func (db *sqlite) getChangeTrackingLastVersion(t tableInfo) (int64, error) {
	var ver int64
	err := db.db.QueryRowx(
		`SELECT change_tracking_last_version
		FROM replication_progress
		WHERE schema_name = ? AND table_name = ?`,
		t.schema, t.name,
	).Scan(&ver)
	if err != nil {
		return 0, fmt.Errorf("sql query: %w", err)
	}

	return ver, err
}
